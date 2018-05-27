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
        self._key_node_dict = {}
        self._key_output_config_dict = {}
        self._node_keys_dict = {}
        self._node_attrs_dict = {}
        self._node_predecessor_dict = {}

    def add_node(self, name, keys=(), predecessor_keys=(), output_configs=(), attrs=None):
        # pylint: disable=protected-access
        if name in self._node_attrs_dict:
            # should be impossible
            raise ValueError("Duplicated node name '{}' for {} and {}."
                             .format(name, self._node_attrs_dict[name], attrs))
        self._node_attrs_dict[name] = attrs

        if len(keys) == 0:
            raise ValueError("No data key for {}.".format(name))
        self._node_keys_dict[name] = tuple(sorted(keys))

        for key in keys:
            if key in self._key_node_dict:
                raise ValueError("Duplicated data key '{}' for {} and {}."
                                 .format(key, self._key_node_dict[key], name))
            self._key_node_dict[key] = name

        for output_config in output_configs:
            key = output_config['key']
            if key in self._key_output_config_dict:
                raise ValueError("Duplicated data key '{}' in {} (alreadly have value {})."
                                 .format(key, name, self._key_output_config_dict[key]))
            self._key_output_config_dict[key] = {
                'handler': output_config['handler'],
                'handler_kwargs': output_config['handler_kwargs'],
            }

        self._node_predecessor_dict[name] = tuple(sorted(set(predecessor_keys)))

    def get_handler_name(self, key):
        return self._key_output_config_dict[key]['handler']

    def match_node(self, key):
        found_node = None
        for data_key, node in six.viewitems(self._key_node_dict):
            if data_key == key:
                if found_node is None:
                    found_node = node
                else:
                    raise ValueError("Duplicated data key '{}' for {} and {}."
                                     .format(key, found_node, node))
        if found_node is None:
            raise KeyError(key)
        return found_node

    def get_node_attr(self, key):
        node = self.match_node(key)
        node_attr = self._node_attrs_dict[node]
        return node_attr

    def _grow_ancestors(self, nx_digraph, root_node_key, predecessor_keys):
        predecessor_keys = {k: k for k in predecessor_keys}
        # grow the graph using DFS
        for template_key, key in six.viewitems(predecessor_keys):
            node = self.match_node(key)

            # for merging node, we use key as the 'key' in nx_digraph
            node_keys = self._node_keys_dict[node]

            if node_keys not in nx_digraph:
                attrs = self._node_attrs_dict[node].copy()
                nx_digraph.add_node(node_keys, **attrs)
                self._grow_ancestors(nx_digraph, node_keys, self._node_predecessor_dict[node])

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
