from os.path import dirname

from mkdir_p import mkdir_p
import six
import networkx as nx

from .data_definition import DataDefinition


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

    def _grow_ancestors(self, nx_digraph, root_node_key, predecessor_defs, key_template_dict=None):
        """
        Parameters
        ----------
        predecessor_defs: Sequence[DataDefinition]
        """
        if key_template_dict is None:
            key_template_dict = {}

        # grow the graph using DFS
        for predecessor_def in predecessor_defs:
            node_attrs = self._key_node_attrs_dict[predecessor_def.key]

            # for merging node, we use key as the 'key' in nx_digraph
            node_data_defs = predecessor_def.replace(
                key=tuple(node_attrs['output_configs'].keys()))

            if node_data_defs not in nx_digraph:
                # TODO: check the argument
                # ancestors of this node has not been grown
                pre_key_template_dict = {
                    DataDefinition(key_template.format(**predecessor_def.args)): key_template
                    for key_template in node_attrs['requirements']}
                nx_digraph.add_node(node_data_defs, **node_attrs)
                requirement_defs = list(pre_key_template_dict.keys())
                self._grow_ancestors(
                    nx_digraph, node_data_defs, requirement_defs, pre_key_template_dict)
            if not nx_digraph.has_edge(node_data_defs, root_node_key):
                # initialize edge
                nx_digraph.add_edge(
                    node_data_defs, root_node_key, data_definitions=set(), template_key_dict={})
            edge_attr = nx_digraph.edges[node_data_defs, root_node_key]
            edge_attr['data_definitions'].add(predecessor_def)
            template_key = key_template_dict.get(predecessor_def, predecessor_def.key)
            edge_attr['template_key_dict'][predecessor_def.key] = template_key

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

    def draw(self, path, data_definitions, root_node_key='root', reverse=False):
        nx_digraph = self.build_directed_graph(data_definitions, root_node_key)
        if reverse:
            nx_digraph.reverse(copy=False)
        draw_dag(nx_digraph, path)
        return nx_digraph
