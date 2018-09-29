from __future__ import print_function, division, absolute_import, unicode_literals
import inspect
from collections import Counter
from copy import deepcopy
try:
    from inspect import signature
except ImportError:
    from funcsigs import signature

import six
import networkx as nx
from bistiming import SimpleTimer

from .dag import DataGraph, draw_dag
from .bundling import DataBundlerMixin
from .data_handlers import (
    MemoryDataHandler,
    H5pyDataHandler,
    PandasHDFDataHandler,
    PickleDataHandler,
)


def check_result_dict_keys(result_dict, expected_key_set, function_name):
    """Check whether the ``result_dict`` match the ``expected_key_set``.

    Raises
    ------
    ValueError
        Raise if the keys in ``result_dict`` doesn't match ``expected_key_set``.
    """
    result_dict_key_set = set(result_dict.keys())
    if expected_key_set != result_dict_key_set:
        # format the error message
        missing_keys = expected_key_set - result_dict_key_set
        redundant_keys = result_dict_key_set - expected_key_set
        detail_messages = []
        if missing_keys:
            detail_messages.append('missing_keys: %s' % missing_keys)
        if redundant_keys:
            detail_messages.append('redundant_keys: %s' % redundant_keys)

        err_msg = ("The return keys of function %s %s is not expected. (%s)"
                   % (function_name,
                      result_dict_key_set if result_dict_key_set else "{}",
                      ', '.join(detail_messages)))
        raise ValueError(err_msg)


class DataGeneratorType(type):

    def __init__(cls, name, bases, attrs):  # noqa
        # pylint: disable=protected-access
        super(DataGeneratorType, cls).__init__(name, bases, attrs)

        attrs = inspect.getmembers(
            cls, lambda a: hasattr(a, '_dagian_output_configs'))

        dag = DataGraph()
        # build the dynamic DAG
        handler_set = set()
        for function_name, function in attrs:
            handler_set.update(config['handler'] for config in function._dagian_output_configs)

            # requirements
            if hasattr(function, '_dagian_requirements'):
                requirements = function._dagian_requirements
            else:
                requirements = ()

            # parameters
            sig = signature(function)
            parameters = []
            for name, p in sig.parameters.items():
                # We only take p.kind in {POSITIONAL_OR_KEYWORD, KEYWORD_ONLY} with valid name.
                if name in ('self', 'upstream_data'):
                    continue
                elif p.kind in (p.POSITIONAL_ONLY, p.VAR_POSITIONAL):
                    raise ValueError(
                        "generator methods in dagian.DataGenerator only accept keyword arguments")
                elif p.kind != p.VAR_KEYWORD:
                    parameters.append(p)

            dag.add_node(
                function_name,
                parameters=parameters,
                requirements=requirements,
                output_configs=function._dagian_output_configs,
            )

        cls._dag = dag
        cls._handler_set = handler_set


def _run_function(function, data_definitions, kwargs):
    with SimpleTimer("Generating {} using {}"
                     .format(data_definitions, function.__name__),
                     end_in_new_line=False):  # pylint: disable=C0330
        result_dict = function(**kwargs)
    return result_dict


def _check_result_dict_type(result_dict, function_name):
    if not (hasattr(result_dict, 'keys')
            and hasattr(result_dict, '__getitem__')):
        raise ValueError("the return value of mehod {} should have "
                         "keys and __getitem__ methods".format(function_name))


class DataGenerator(six.with_metaclass(DataGeneratorType, DataBundlerMixin)):

    def __init__(self, handlers):
        handler_set = set(six.viewkeys(handlers))
        if handler_set != self._handler_set:
            redundant_handlers_set = handler_set - self._handler_set
            lacked_handlers_set = self._handler_set - handler_set
            raise ValueError('Handler set mismatch. {} redundant and {} lacked.'
                             .format(redundant_handlers_set,
                                     lacked_handlers_set))
        self._handlers = handlers

    def get_handler(self, key):
        handler_name = self._dag.get_handler_name(key)
        handler = self._handlers[handler_name]
        return handler

    @classmethod
    def check_data_definition(cls, data_definition):
        """Check whether a data definition is valid and set the defaut arguments.

        Parameters
        ----------
        data_definition : DataDefinition

        Returns
        -------
        valid_data_definition : DataDefinition
            Data definition with default values filled.
        """
        key_node_attrs_dict = cls._dag._key_node_attrs_dict
        if data_definition.key not in key_node_attrs_dict:
            raise KeyError("Data key %s doesn't exist." % data_definition.key)
        parameters = key_node_attrs_dict[data_definition.key]['parameters']

        # check parameters
        defined_params = set(data_definition.args.keys())
        valid_params = set(param.name for param in parameters)
        required_params = set(param.name for param in parameters
                              if param.default is param.empty)
        if not defined_params.issubset(valid_params):
            unexpected_args = defined_params - valid_params
            raise ValueError("Data definition %s has %d unexpected arguments %s."
                             % (data_definition, len(unexpected_args), unexpected_args))
        if not required_params.issubset(defined_params):
            missing_args = required_params - defined_params
            raise ValueError("Data definition %s misses %d required arguments %s."
                             % (data_definition, len(missing_args), missing_args))

        # fill default args
        valid_args = {param.name: data_definition.args.get(param.name, param.default)
                      for param in parameters}
        valid_data_definition = data_definition.replace(args=valid_args)
        return valid_data_definition

    @classmethod
    def check_data_definitions(cls, data_definitions):
        """``check_data_definition()`` with ``Sequence`` input and output.

        Parameters
        ----------
        data_definitions : Sequence[DataDefinition]

        Returns
        -------
        valid_data_definitions : Sequence[DataDefinition]
            Data definitions with default values filled.
        """
        valid_data_definitions = [cls.check_data_definition(data_def)
                                  for data_def in data_definitions]
        return valid_data_definitions

    def get(self, data_definition):
        data_definition = self.check_data_definition(data_definition)
        handler = self.get_handler(data_definition.key)
        data = handler.get(data_definition)
        return data

    def _dag_prune_can_skip(self, nx_digraph, generation_order):
        for node in reversed(generation_order):
            node_attrs = nx_digraph.node[node]
            key_info_dict = {key: {'handler': self._handlers[config['handler']]}
                             for key, config in six.viewitems(node_attrs['output_configs'])}
            node_attrs['skipped'] = True
            for target_node, edge_attr in nx_digraph.succ[node].items():
                if nx_digraph.node[target_node]['skipped']:
                    edge_attr['skipped_data'] = edge_attr['data_definitions']
                    edge_attr['nonskipped_data'] = set()
                else:
                    required_data_defs = edge_attr['data_definitions']
                    edge_attr['skipped_data'] = set()
                    edge_attr['nonskipped_data'] = set()
                    for required_data_def in required_data_defs:
                        key_info = key_info_dict[required_data_def.key]
                        if 'can_skip' not in key_info:
                            key_info['can_skip'] = key_info['handler'].can_skip(required_data_def)
                        if key_info['can_skip']:
                            edge_attr['skipped_data'].add(required_data_def)
                        else:
                            edge_attr['nonskipped_data'].add(required_data_def)
                    if len(edge_attr['nonskipped_data']) > 0:
                        node_attrs['skipped'] = False

    def build_involved_dag(self, data_definitions):
        # get the nodes and edges that will be considered during the generation
        data_definitions = self.check_data_definitions(data_definitions)
        involved_dag = self._dag.build_directed_graph(data_definitions, root_node_key='generate')
        generation_order = list(nx.topological_sort(involved_dag))[:-1]
        involved_dag.node['generate']['skipped'] = False
        self._dag_prune_can_skip(involved_dag, generation_order)
        return involved_dag, generation_order

    def draw_involved_dag(self, path, data_definitions):
        involved_dag, _ = self.build_involved_dag(data_definitions)
        draw_dag(involved_dag, path)

    def _get_upstream_data(self, dag, data_definitions):
        data = {}
        name_counter = Counter()
        for source_node, edge_attrs in dag.pred[data_definitions].items():
            source_output_configs = dag.nodes[source_node]['output_configs']
            for pred_def in edge_attrs['data_definitions']:
                source_handler_str = source_output_configs[pred_def.key]['handler']
                source_handler = self._handlers[source_handler_str]
                source_data = source_handler.get(pred_def)
                if name_counter[pred_def.name] == 0:
                    data[pred_def.name] = source_data
                elif name_counter[pred_def.name] == 1:
                    data[pred_def.name] = [data[pred_def.name], source_data]
                else:
                    data[pred_def.name].append(source_data)
                name_counter[pred_def.name] += 1
        return data

    def _generate_one(self, dag, data_definitions, func_name, output_configs):
        # prepare kwargs for function
        data = self._get_upstream_data(dag, data_definitions)
        if data:
            function_kwargs = {'upstream_data': data}
        else:
            function_kwargs = {}
        if data_definitions.args:
            function_kwargs.update(deepcopy(data_definitions.args._dict))

        # TODO: add handler-specific arguments
        # handler = self._handlers[handler_key]
        # function_kwargs = handler.get_function_kwargs(
        #     data_definitions=data_definitions,
        #     data=data,
        #     **handler_kwargs
        # )
        function = getattr(self, func_name)
        result_dict = _run_function(function, data_definitions, function_kwargs)
        self.close()

        if result_dict is None:
            result_dict = {}

        # check result_dict
        _check_result_dict_type(result_dict, func_name)
        expected_keys = set(
            key for key, config in six.viewitems(output_configs)
            if (self._handlers[config['handler']]
                .is_return_data_expected(**config['handler_kwargs'])))
        check_result_dict_keys(result_dict, expected_keys, func_name)

        # write data
        for key in sorted(expected_keys):
            config = output_configs[key]
            data_definition = data_definitions.replace(key=key)
            self._handlers[config['handler']].write_data(
                data_definition, result_dict[key], **config['handler_kwargs'])

    def generate(self, data_definitions, dag_output_path=None):
        """
        Parameters
        ----------
        data_definitions: Sequence[DataDefinition]
        """
        involved_dag, generation_order = self.build_involved_dag(data_definitions)
        if dag_output_path is not None:
            draw_dag(involved_dag, dag_output_path)

        # generate data
        for data_definitions in generation_order:
            node_attrs = involved_dag.nodes[data_definitions]
            if node_attrs['skipped']:
                continue
            self._generate_one(
                involved_dag, data_definitions, node_attrs['func_name'],
                node_attrs['output_configs'])

        return involved_dag

    def close(self):
        for handler in six.viewvalues(self._handlers):
            handler.close()

    @classmethod
    def draw_dag(cls, path, data_definitions):
        # pylint: disable=protected-access
        data_definitions = cls.check_data_definitions(data_definitions)
        dag = cls._dag.draw(path, data_definitions, root_node_key='generate')
        if not nx.is_directed_acyclic_graph(dag):
            print("Warning! The graph is not acyclic!")


class FeatureGenerator(DataGenerator):

    def __init__(self, handlers=None, h5py_hdf_path=None, pandas_hdf_path=None,
                 pickle_dir=None):
        if handlers is None:
            handlers = {}
        if 'memory' in self._handler_set and 'memory' not in handlers:
            handlers['memory'] = MemoryDataHandler()
        if 'h5py' in self._handler_set and 'h5py' not in handlers:
            if h5py_hdf_path is None:
                raise ValueError("h5py_hdf_path should be specified "
                                 "when initiating FeatureGenerator.")
            handlers['h5py'] = H5pyDataHandler(h5py_hdf_path)
        if 'pandas_hdf' in self._handler_set and 'pandas_hdf' not in handlers:
            if pandas_hdf_path is None:
                raise ValueError("pandas_hdf_path should be specified "
                                 "when initiating FeatureGenerator.")
            handlers['pandas_hdf'] = PandasHDFDataHandler(pandas_hdf_path)
        if 'pickle' in self._handler_set and 'pickle' not in handlers:
            if pickle_dir is None:
                raise ValueError("pickle_dir should be specified "
                                 "when initiating FeatureGenerator.")
            handlers['pickle'] = PickleDataHandler(pickle_dir)
        super(FeatureGenerator, self).__init__(handlers)
