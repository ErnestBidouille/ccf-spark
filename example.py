from pyspark import SparkContext
from ccf_spark import CcfSpark, GraphGenerator

sc = SparkContext()
graph = GraphGenerator(10000, 7500)
p = CcfSpark(
    sc,
    secondary_sorting=True,
    graph=graph,
)
p.iterate_all()
print(p.print())
