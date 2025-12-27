from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Step 1") \
    .master("local[*]") \
    .getOrCreate()
    
data = [("u1", "click"), ("u2", "view"), ("u3", "click")]
df = spark.createDataFrame(data, ["user_id", "event_type"])

print("Partitions: ", df.rdd.getNumPartitions())

filtered = df.filter(df.event_type == "click")

print("Filtered count: ", filtered.count())