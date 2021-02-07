from pyspark import SparkContext
from ccf_spark import CcfSpark


sc = SparkContext()
p = CcfSpark(
    sc,
    secondary_sorting=True,
)
p.iterate_all()
print(p.print())