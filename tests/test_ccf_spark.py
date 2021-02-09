import os
import networkx as nx
from pyspark import SparkContext
from ccf_spark import CcfSpark, GraphGenerator, Ccf
data_path = os.path.join(os.path.dirname(__file__), 'data')
sc = SparkContext.getOrCreate()


def test_ccf_ccf_spar_init():
    graph = CcfSpark(sc, secondary_sorting=True)
    assert graph.graph.count() == 350
    assert graph.secondary_sorting
    assert graph.iterator == Ccf.IterateSecondarySorting
    graph = CcfSpark(
        sc,
        file_path=os.path.join(data_path, 'test_graph.txt'),
    )
    assert graph.graph.count() == 6
    assert not graph.secondary_sorting
    assert graph.iterator == Ccf.Iterate
    graph = CcfSpark(
        sc,
        file_path=os.path.join(data_path, 'test_second_graph.txt'),
        separator='\t',
    )
    assert graph.graph.count() == 6
    graph_generator = GraphGenerator.generate_ccf_random_graph(12, 15)
    graph = CcfSpark(
        sc,
        graph=graph_generator.graph,
    )
    assert graph.graph.count() == 15


def test_ccf_ccf_spark_iterate():
    graph = CcfSpark(
        sc,
        file_path=os.path.join(data_path, 'test_graph.txt'),
    )
    graph.iterate()
    expected = [(2, 1), (3, 1), (4, 1), (3, 2), (4, 2), (5, 2), (5, 4), (7, 6),
                (8, 6), (8, 7)]
    assert len(graph.print()) == len(expected)
    assert set(graph.print()) == set(expected)
    graph = CcfSpark(
        sc,
        file_path=os.path.join(data_path, 'test_graph.txt'),
        secondary_sorting=True
    )
    graph.iterate()
    expected = [(2, 1), (3, 1), (4, 1), (3, 2), (4, 2), (5, 2), (5, 4), (7, 6),
                (8, 6), (8, 7)]
    assert len(graph.print()) == len(expected)
    assert set(graph.print()) == set(expected)


def test_ccf_ccf_spark_iterate_all():
    graph = CcfSpark(
        sc,
        file_path=os.path.join(data_path, 'test_graph.txt'),
    )
    graph.iterate_all()
    expected = [(2, 1), (3, 1), (4, 1), (5, 1), (7, 6), (8, 6)]
    assert len(graph.print()) == len(expected)
    assert set(graph.print()) == set(expected)
    graph = CcfSpark(
        sc,
        file_path=os.path.join(data_path, 'test_graph.txt'),
        secondary_sorting=True
    )
    graph.iterate_all()
    expected = [(2, 1), (3, 1), (4, 1), (5, 1), (7, 6), (8, 6)]
    assert len(graph.print()) == len(expected)
    assert set(graph.print()) == set(expected)


def test_ccf_ccf_spark_print():
    graph = CcfSpark(
        sc,
        file_path=os.path.join(data_path, 'test_graph.txt'),
    )
    assert graph.print() == graph.graph.collect()


def test_ccf_ccf_spark_number_of_connected_components():
    graph = CcfSpark(
        sc,
        file_path=os.path.join(data_path, 'test_graph.txt'),
    )
    assert graph.number_of_connected_components() == 2
    graph_generator = GraphGenerator.generate_ccf_random_graph(15,20)
    graph = CcfSpark(
        sc, 
        graph = graph_generator,
    )
    assert graph.number_of_connected_components() == nx.number_connected_components(graph_generator.graph)
