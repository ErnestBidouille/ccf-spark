from networkx.classes.graph import Graph
from pyspark import SparkContext
from ccf_spark.graph_generator import GraphGenerator
from ccf_spark.ccf import Ccf

CCF_DEDUP = Ccf.Dedup
CCF_ITERATE = Ccf.Iterate
CCF_ITERATE_SECONDARY_SORTING = Ccf.IterateSecondarySorting


class CcfSpark:
    def __init__(self,
                 sc: SparkContext,
                 secondary_sorting: bool = False,
                 graph: Graph = None,
                 file_path: str = None):
        self.sc = sc
        self.iterator = CCF_ITERATE_SECONDARY_SORTING if secondary_sorting else CCF_ITERATE
        self.secondary_sorting = secondary_sorting
        if graph:
            self.graph = sc.parallelize(self.graph.edges)
        elif file_path:
            # File line format expected : <int> <int>
            self.graph = sc.textFile(file_path).map(
                lambda x: tuple(map(int,
                                    x.split('  ')[:2])))
        else:
            self.graph = sc.parallelize(
                GraphGenerator.generate_random_graph(500, 350).edges)

    def iterate(self):
        accumulator = self.sc.accumulator(0)
        iterator = self.iterator  # To avoid SPARK-5063 error.
        self.graph = self.graph.flatMap(iterator.map).groupByKey()
        if self.secondary_sorting:
            self.graph = self.graph.map(lambda x: (x[0], sorted(x[1])))
        self.graph = self.graph.flatMap(
            lambda x, accumulator=accumulator: iterator.reduce(x, accumulator
                                                               )).sortByKey()
        self.graph = self.graph.map(CCF_DEDUP.map).groupByKey()
        self.graph = self.graph.map(CCF_DEDUP.reduce)
        return accumulator.value

    def iterate_all(self):
        while True:
            new_pairs = self.iterate()
            if not new_pairs:
                break

    def print(self):
        return self.graph.collect()


