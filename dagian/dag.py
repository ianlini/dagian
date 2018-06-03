from os.path import dirname

from past.builtins import basestring
import six
from mkdir_p import mkdir_p
import networkx as nx
from frozendict import frozendict


class DataDefinition(frozendict):
    def __init__(self, key, args=None):
        self._key = key
        if args is None:
            self._args = frozendict()
        else:
            self._args = frozendict(args)
        super(DataDefinition, self).__init__(key=key, args=self._args)

    @property
    def key(self):
        return self._key

    @property
    def args(self):
        return self._args

    def replace(self, key=None, args=None):
        if key is None:
            key = self._key
        if args is None:
            args = self._args
        return DataDefinition(key=key, args=args)


def to_data_definitions(raw_data_definitions):
    """
    Parameters
    ----------
    raw_data_definitions: Union[str, Mapping]
        Accept 2 formats of data definition:
        1. str format
        2. Mapping with key ``key`` and ``args``
    """
    if isinstance(raw_data_definitions, basestring):
        raw_data_definitions = (raw_data_definitions,)
    data_definitions = []
    for raw_data_definition in raw_data_definitions:
        if isinstance(raw_data_definition, basestring):
            data_definition = DataDefinition(key=raw_data_definition)
        elif isinstance(raw_data_definition, dict):
            data_definition = DataDefinition(**raw_data_definition)
        else:
            raise ValueError("Data definition format not supported: {}."
                             .format(raw_data_definition))
        data_definitions.append(data_definition)
    return data_definitions


def draw_dag(nx_dag, path):
    if dirname(path) != '':
        mkdir_p(dirname(path))
    agraph = nx.nx_agraph.to_agraph(nx_dag)
    for edge in agraph.edges_iter():
        if edge.attr['nonskipped_data'] is None:
            edge.attr['label'] = edge.attr['keys']
        else:
            edge.attr['label'] = ""
            if edge.attr['nonskipped_data'] not in ["set()", "set([])"]:
                edge.attr['label'] += edge.attr['nonskipped_data']
            if (edge.attr['skipped_data'] not in ["set()", "set([])"]
                    and edge.attr['skipped_data'] is not None):
                edge.attr['label'] += "(%s skipped)" % edge.attr['skipped_data']
    for node in agraph.nodes_iter():
        if node.attr['skipped'] == "True":
            node.attr['label'] = node.attr['func_name'] + " (skipped)"
            node.attr['fontcolor'] = 'grey'
        else:
            node.attr['label'] = node.attr['func_name']
    agraph.layout('dot')
    agraph.draw(path)


class DataGraph(object):
    """Directed graph that each node is represented by regular expression.

    We can use a string to find a node that has regex matching the string. The
    matching groups can also be the arguments to define the edge. Because the
    predecessor is matched depending on regex, the graph will be dynamically built
    given some entry points.
    """

    def __init__(self):
        self._key_output_config_dict = {}
        self._key_node_attrs_dict = {}

    def add_node(self, name, parameters, requirements, output_configs):
        # pylint: disable=protected-access
        # format better data structure
        parameters = tuple(sorted(set(parameters)))
        requirements = tuple(sorted(set(requirements)))
        output_config_dict = {
            config['key']: {
                'handler': config['handler'],
                'handler_kwargs': config['handler_kwargs'],
            }
            for config in output_configs
        }
        if not output_config_dict:
            raise ValueError("No data key for {}.".format(name))

        # build index
        for key, output_config in six.viewitems(output_config_dict):
            if key in self._key_output_config_dict:
                raise ValueError("Duplicated data key '{}' in {} (alreadly have value {})."
                                 .format(key, name, self._key_output_config_dict[key]))
            self._key_output_config_dict[key] = output_config

        node_attrs = {
            'func_name': name,
            'parameters': parameters,
            'requirements': requirements,
            'output_configs': output_config_dict,
        }

        for key in output_config_dict.keys():
            if key in self._key_node_attrs_dict:
                raise ValueError("Duplicated data key '{}' for {} and {}."
                                 .format(key, self._key_node_attrs_dict[key], node_attrs))
            self._key_node_attrs_dict[key] = node_attrs

    def get_handler_name(self, key):
        return self._key_output_config_dict[key]['handler']

    def _grow_ancestors(self, nx_digraph, root_node_key, predecessor_defs):
        """
        Parameters
        ----------
        predecessor_defs: Sequence[DataDefinition]
        """
        # grow the graph using DFS
        for predecessor_def in predecessor_defs:
            node_attrs = self._key_node_attrs_dict[predecessor_def.key]

            # for merging node, we use key as the 'key' in nx_digraph
            node_data_defs = tuple(
                predecessor_def.replace(key=key) for key in node_attrs['output_configs'].keys())

            if node_data_defs not in nx_digraph:
                # ancestors of this node has not been grown
                nx_digraph.add_node(node_data_defs, **node_attrs)
                requirement_defs = to_data_definitions(node_attrs['requirements'])
                self._grow_ancestors(nx_digraph, node_data_defs, requirement_defs)

            if not nx_digraph.has_edge(root_node_key, node_data_defs):
                # initialize edge
                nx_digraph.add_edge(
                    root_node_key, node_data_defs, data_definitions=set(), template_keys={})
            edge_attr = nx_digraph.edges[root_node_key, node_data_defs]
            edge_attr['data_definitions'].add(predecessor_def)
            edge_attr['template_keys'][predecessor_def.key] = predecessor_def

    def build_directed_graph(self, data_definitions, root_node_key='root'):
        """
        Parameters
        ----------
        data_definitions: Sequence[DataDefinition]
        """
        nx_digraph = nx.DiGraph()
        nx_digraph.add_node(root_node_key, func_name=root_node_key)
        self._grow_ancestors(nx_digraph, root_node_key, data_definitions)
        return nx_digraph

    def draw(self, path, keys, root_node_key='root', reverse=False):
        nx_digraph = self.build_directed_graph(keys, root_node_key)
        if reverse:
            nx_digraph.reverse(copy=False)
        draw_dag(nx_digraph, path)
        return nx_digraph
