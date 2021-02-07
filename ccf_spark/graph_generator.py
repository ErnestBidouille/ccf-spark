import networkx as nx


class GraphGenerator:
    def __init__(self, nodes: int, edges: int):
        self.graph = self._generate_random_graph(nodes, edges)

    def generate_new_random_graph(self, nodes: int, edges: int):
        self.graph = self._generate_random_graph(nodes, edges)

    def _generate_random_graph(self, nodes: int, edges: int):
        return nx.gnm_random_graph(nodes, edges)

    def remove_null_degres_nodes(self):
        copy = self.graph.copy()
        self.graph = nx.subgraph_view(
            copy,
            filter_node=lambda node: nx.degree(copy, node) != 0,
        )

    def save(self, path):
        with open(path, 'wb') as f:
            nx.write_edgelist(self.graph, f)

    def draw(self):
        nx.draw(self.graph, with_labels=True)

    @staticmethod
    def generate_random_graph(nodes, edges):
        graph = GraphGenerator(nodes, edges)
        graph.remove_null_degres_nodes()
        return graph

    @property
    def edges(self):
        return self.graph.edges

    @property
    def nodes(self):
        return self.graph.nodes
