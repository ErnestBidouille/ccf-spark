from ccf_spark import GraphGenerator

def test_init():
    nodes = 10
    edges = 15
    graph = GraphGenerator(nodes,edges)
    assert len(graph.nodes) == nodes and len(graph.edges) == edges

def test_generate_new_random_graph():
    nodes = 10
    edges = 15
    graph = GraphGenerator(17,8)
    graph.generate_new_random_graph(nodes, edges)
    assert len(graph.nodes) == nodes and len(graph.edges) == edges

def test_remove_null_degres_nodes():
    nodes = 15
    edges = 1
    graph = GraphGenerator(nodes,edges)
    graph.remove_null_degres_nodes()
    assert len(graph.nodes) == 2

def test_save(tmpdir):
    p = tmpdir.mkdir("sub").join("graph.txt")
    graph = GraphGenerator(3, 3)
    graph.save(p)
    assert p.read() == "0 1 {}\n0 2 {}\n1 2 {}\n"
    assert len(tmpdir.listdir()) == 1

def test_generate_ccf_random_graph():
    graph = GraphGenerator.generate_ccf_random_graph(15, 1)
    assert len(graph.nodes) == 2

def test_number_connected_components():
    graph = GraphGenerator(10,1)
    assert graph.number_connected_components == 9
    nodes = 10
    edges = nodes * (nodes - 1)/2
    graph = GraphGenerator(nodes, edges)
    assert graph.number_connected_components == 1
