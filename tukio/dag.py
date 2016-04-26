"""
Build DAGs
"""
from copy import deepcopy


class DAGValidationError(Exception):
    pass


class DAG(object):
    """
    Directed Acyclic Graph (DAG) implementation. This implementation uses an
    adjacency list to represent the graph.
    """
    def __init__(self):
        self.graph = {}

    def add_node(self, node_id):
        """
        Add a new node in the graph.
        """
        if node_id in self.graph:
            raise ValueError("node '{}' already exists".format(node_id))
        self.graph[node_id] = set()

    def delete_node(self, node_id):
        """
        Delete a node and all edges referencing it.
        """
        if node_id not in self.graph:
            raise KeyError("node '{}' does not exist".format(node_id))
        self.graph.pop(node_id)
        # Remove all edges referencing the node just removed
        for edges in self.graph.values():
            if node_id in edges:
                edges.remove(node_id)

    def add_edge(self, predecessor, successor):
        """
        Add a directed edge between two specified nodes: from predecessor to
        successor.
        """
        if predecessor not in self.graph or successor not in self.graph:
            raise KeyError('nodes do not exist in graph')
        self.graph[predecessor].add(successor)
        try:
            self.validate()
        except DAGValidationError as exc:
            # Rollback the last update if it breaks the DAG
            self.graph[predecessor].remove(successor)
            raise exc

    def delete_edge(self, predecessor, successor, graph=None):
        """
        Delete an edge from the graph.
        """
        graph = graph or self.graph
        if successor not in graph.get(predecessor, []):
            raise KeyError('this edge does not exist in graph')
        graph[predecessor].remove(successor)

    def predecessors(self, node, graph=None):
        """
        Returns the list of all predecessors of the given node
        """
        graph = graph or self.graph
        if node not in graph:
            raise KeyError('node %s is not in graph' % node)
        return [key for key in graph if node in graph[key]]

    def successors(self, node, graph=None):
        """
        Returns the list of all successors of the given node
        """
        graph = graph or self.graph
        if node not in graph:
            raise KeyError('node %s is not in graph' % node)
        return list(graph[node])

    def leaves(self):
        """
        Returns the list of all leaves (nodes with no successor)
        """
        return [key for key in self.graph if not self.graph[key]]

    @classmethod
    def from_dict(cls, graph):
        """
        Build a new DAG from the given dict.
        The dictionary takes the form of {node-a: [node-b, node-c]}
        """
        dag = cls()
        # Create all nodes
        for node in graph.keys():
            dag.add_node(node)
        # Build all edges
        for node, successors in graph.items():
            if not isinstance(successors, list):
                raise TypeError('dict values must be lists')
            for succ in successors:
                dag.add_edge(node, succ)
        return dag

    def root_nodes(self):
        """
        Returns the list of all root nodes (aka nodes without predecessor).
        """
        all_nodes = set(self.graph.keys())
        successors = set()
        for nodes in self.graph.values():
            successors.update(nodes)
        root_nodes = list(all_nodes - successors)
        if not root_nodes:
            raise DAGValidationError('no root node found')
        return root_nodes

    def validate(self):
        """
        Validate the DAG by looking for unlinked nodes and looking for cycles
        in the graph. If there is no unlinked node and no cycle the DAG is
        valid.
        """
        self.root_nodes()
        self._toposort()
        return 'graph is a valid DAG'

    def is_valid(self):
        """
        Return `True` if the graph is a valid DAG, else return `False`.
        """
        try:
            self.validate()
        except DAGValidationError:
            return False
        return True

    def _toposort(self):
        """
        Topological ordering of the DAG using Kahn's algorithm. This algorithm
        detects cycles, hence ensures the graph is a DAG.
        """
        graph = deepcopy(self.graph)
        sorted_nodes = []
        root_nodes = set(self.root_nodes())
        while root_nodes:
            root = root_nodes.pop()
            sorted_nodes.append(root)
            # Walk through the successors of `root` to remove all its outgoing
            # edges.
            for node in graph[root].copy():
                self.delete_edge(root, node, graph)
                if not self.predecessors(node, graph):
                    root_nodes.add(node)
        if self.edges(graph):
            raise DAGValidationError('graph is not acyclic')
        else:
            return sorted_nodes

    def edges(self, graph=None):
        """
        Return a list of all edges in the graph (without duplicates)
        """
        graph = graph or self.graph
        edges = set()
        for node in graph:
            for successor in graph[node]:
                edges.add((node, successor))
        return list(edges)


if __name__ == '__main__':
    graph = {
        'A': ['B'],
        'B': ['C', 'D', 'E'],
        'C': ['E'],
        'D': ['E'],
        'E': [],
        'F': []
    }
    dag = DAG.from_dict(graph)
    print("root nodes are: {}".format(dag.root_nodes()))
    print("DAG is ok: {}".format(dag.graph))
