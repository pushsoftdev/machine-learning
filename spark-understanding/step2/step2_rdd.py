# RDD vs DataFrame vs Spark SQL
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Step 2") \
    .master("local[*]") \
    .getOrCreate()
    
rdd = spark.sparkContext.parallelize([1, 2, 3, 4])
rdd2 = rdd.map(lambda x: x * 2)

print("RDD2 Collect", rdd2.collect())