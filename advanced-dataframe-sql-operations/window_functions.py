from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, lag, unix_timestamp,when, sum as spark_sum
import time

spark = SparkSession.builder \
    .appName("Window Functions") \
    .master("local[*]") \
    .getOrCreate()

data = [
    ("u1", "click", "2025-12-27 10:00:00"),
    ("u1", "view", "2025-12-27 10:05:00"),
    ("u1", "click", "2025-12-27 10:45:00"),
    ("u2", "view", "2025-12-27 11:00:00"),
    ("u2", "click", "2025-12-27 11:20:00"),
]

# Create the dataframe
df = spark.createDataFrame(data, ["user_id", "event_type", "event_time"])

# Create a dataframe with event_time column of unix timestamp
df = df.withColumn("event_time", unix_timestamp("event_time"))

# Create a Window
window = Window.partitionBy("user_id").orderBy("event_time")

df = df.withColumn("prev_event_time", lag("event_time").over(window)) \
    .withColumn("time_diff_min", (col("event_time") - col("prev_event_time")) / 60)

window = Window.partitionBy("user_id").orderBy("event_time")

df = df.withColumn(
    "new_session",
    when(col("prev_event_time").isNull(), 1) \
    .when(col("time_diff_min") > 30, 1) \
    .otherwise(0)
)

df = df.withColumn("session_id", spark_sum("new_session").over(window))

df.show()