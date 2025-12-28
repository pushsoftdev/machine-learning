from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, lag, unix_timestamp,when, sum as spark_sum, min, max,count, countDistinct, to_date, from_unixtime, concat_ws
import time
import os

# Command to execute
# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1,org.apache.hadoop:hadoop-aws:3.4.1,com.amazonaws:aws-java-sdk-bundle:1.12.262,mysql:mysql-connector-java:8.0.33 session_metrics.py

spark = SparkSession.builder \
    .appName("Window Functions") \
    .master("local[*]") \
    .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY")) \
    .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_KEY")) \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .config("spark.hadoop.fs.s3a.endpoint.region", os.getenv("AWS_DEFAULT_REGION")) \
    .config('spark.jars.packages', "org.apache.hadoop:hadoop-aws:3.4.1,com.amazonaws:aws-java-sdk-bundle:1.11.1026,org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1,mysql:mysql-connector-java:8.0.33") \
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

# Calculate session_start, session_end, total_eventss, session_duration, 
# is_bounce

session_df = df.groupBy("user_id", "session_id") \
    .agg(
        min("event_time").alias("session_start"),
        max("event_time").alias("session_end"),
        count("*").alias("total_events"),
        countDistinct("event_type").alias("unique_event_types")
    ) \
    .withColumn("session_duration_sec", col("session_end") - col("session_start"))
    
session_df = session_df \
    .withColumn(
        "is_bounce", 
        when(session_df.unique_event_types == 1, 1) \
        .otherwise(0)
    )
    
session_df.show()

# session_df.selectExpr(
#     "min(s_duration_sec) as min_session_duration",
#     "max(s_duration_sec) as max_session_duration"
# ).show()

session_df = session_df \
    .withColumn("session_date", to_date(from_unixtime("session_start"))) \
    .withColumn("session_key", concat_ws("-", "user_id", "session_id", "session_date")) \
    .withColumn("session_start", from_unixtime("session_start")) \
    .withColumn("session_end", from_unixtime("session_end"))

# Write it to S3
session_df.write \
    .mode("append") \
    .partitionBy("session_date") \
    .parquet("s3a://ml-user-events/website-sessions/")

# Write it to MySQL
jdbc_url = "jdbc:mysql://localhost:3306/kafka_demo"
table = "website_sessions"

properties = {"user": "root", "password": "", "driver": "com.mysql.cj.jdbc.Driver"}

session_df \
    .repartition(4) \
    .write \
    .jdbc(url=jdbc_url, table=table, mode="append", properties=properties)