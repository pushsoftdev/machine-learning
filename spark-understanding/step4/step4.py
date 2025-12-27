from pyspark.sql import SparkSession
import time

spark = SparkSession.builder \
    .appName("Step 2") \
    .master("local[*]") \
    .getOrCreate()
    
data = [("u1", "click")] * 10000 + \
    [("u2", "view")] * 10000 + \
    [("u3", "click")] * 10000
    
df = spark.createDataFrame(data, ["user_id", "event_type"])

filtered = df.filter(df.event_type == "click")

# filtered.cache()

print("JP: Count :", filtered.count())
print("JP: Take: ", filtered.take(5))

time.sleep(300)

spark.stop()