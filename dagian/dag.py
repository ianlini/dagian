from os.path import dirname

import six
from mkdir_p import mkdir_p
import networkx as nx


def draw_dag(nx_dag, path):
    if dirname(path) != '':
        mkdir_p(dirname(path))
    agraph = nx.nx_agraph.to_agraph(nx_dag)
    for edge in agraph.edges_iter():
        if edge.attr['nonskipped_keys'] is None:
            edge.attr['label'] = edge.attr['keys']
        else:
            edge.attr['label'] = ""
            if edge.attr['nonskipped_keys'] not in ["set()", "set([])"]:
                edge.attr['label'] += edge.attr['nonskipped_keys']
            if (edge.attr['skipped_keys'] not in ["set()", "set([])"]
                    and edge.attr['skipped_keys'] is not None):
                edge.attr['label'] += "(%s skipped)" % edge.attr['skipped_keys']
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

    def _grow_ancestors(self, nx_digraph, root_node_key, predecessor_keys):
        predecessor_keys = {k: k for k in predecessor_keys}
        # grow the graph using DFS
        for template_key, key in six.viewitems(predecessor_keys):
            node_attrs = self._key_node_attrs_dict[key]

            # for merging node, we use key as the 'key' in nx_digraph
            node_keys = tuple(sorted(node_attrs['output_configs'].keys()))

            if node_keys not in nx_digraph:
                nx_digraph.add_node(node_keys, **node_attrs)
                self._grow_ancestors(nx_digraph, node_keys, node_attrs['requirements'])

            if not nx_digraph.has_edge(root_node_key, node_keys):
                # initialize edge
                nx_digraph.add_edge(root_node_key, node_keys, keys=set(), template_keys={})
            edge_attr = nx_digraph.edges[root_node_key, node_keys]
            edge_attr['keys'].add(key)
            edge_attr['template_keys'][template_key] = key

    def build_directed_graph(self, keys, root_node_key='root'):
        nx_digraph = nx.DiGraph()
        nx_digraph.add_node(root_node_key, func_name=root_node_key)
        self._grow_ancestors(nx_digraph, root_node_key, keys)
        return nx_digraph

    def draw(self, path, keys, root_node_key='root', reverse=False):
        nx_digraph = self.build_directed_graph(keys, root_node_key)
        if reverse:
            nx_digraph.reverse(copy=False)
        draw_dag(nx_digraph, path)
        return nx_digraph
