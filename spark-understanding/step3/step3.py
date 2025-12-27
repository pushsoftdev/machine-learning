from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Step 1") \
    .master("local[*]") \
    .getOrCreate()
    
data = [("u1", "click")] * 1000 + \
    [("u2", "view")] * 1000 + \
    [("u3", "click")] * 1000
    
df = spark.createDataFrame(data, ["user_id", "event_type"])

print("JP: Initial Partitions: ", df.rdd.getNumPartitions())

grouped = df.groupBy("event_type").count()

grouped.show()
