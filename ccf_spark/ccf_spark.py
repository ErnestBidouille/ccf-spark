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
                 file_path: str = None,
                 separator: str = ' '):
        self.sc = sc
        self.iterator = CCF_ITERATE_SECONDARY_SORTING if secondary_sorting else CCF_ITERATE
        self.secondary_sorting = secondary_sorting
        if graph:
            self.graph = sc.parallelize(graph.edges)
        elif file_path:
            # File line format expected : <int>%separator%<int>
            self.graph = sc.textFile(file_path).map(
                lambda x: tuple(map(int,
                                    x.split(separator)[:2])))
        else:
            self.graph = sc.parallelize(
                GraphGenerator.generate_ccf_random_graph(500, 350).edges)
        self.iterated = False
        self._accumulator = self.sc.accumulator(0)

    def iterate(self, with_distinct: bool = False) -> int:
        self._accumulator.value = 0
        iterator = self.iterator  # To avoid SPARK-5063 error.
        self.graph = self.graph.flatMap(iterator.map).groupByKey()
        if self.secondary_sorting:
            self.graph = self.graph.map(lambda x: (x[0], sorted(x[1])))
        self.graph = self.graph.flatMap(
            lambda x, accumulator=self._accumulator: iterator.reduce(
                x, accumulator)).sortByKey()
        if with_distinct:
            self.graph = self.graph.distinct()
        else:
            self.graph = self.graph.map(CCF_DEDUP.map).groupByKey()
            self.graph = self.graph.map(CCF_DEDUP.reduce)
        return self._accumulator.value

    def iterate_all(self, with_distinct: bool = False) -> None:
        while True:
            new_pairs = self.iterate(with_distinct)
            if not new_pairs:
                break
        self.iterated = True

    def print(self) -> list:
        return self.graph.collect()

    def number_of_connected_components(self) -> int:
        """
            Can only be used after iterate_all()
        """
        if not self.iterated:
            self.iterate_all()
        return self.graph.map(lambda x: (x[1], 1)).reduceByKey(
            lambda a, b: (a + b)).count()
