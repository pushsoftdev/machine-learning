from pyspark.sql import SparkSession
import os

spark = SparkSession.builder \
    .appName("S3 Reader") \
    .config('spark.hadoop.fs.s3a.access.key', os.getenv("AWS_ACCESS_KEY")) \
    .config('spark.hadoop.fs.s3a.secret.key', os.getenv("AWS_SECRET_KEY")) \
    .config('spark.hadoop.fs.s3a.endpoint', "s3.amazonaws.com") \
    .config('spark.hadoop.fs.s3a.endpoint.region', os.getenv("AWS_DEFAULT_REGION")) \
    .config('spark.hadoop.fs.s3a.impl', "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config('spark.jars.packages', "org.apache.hadoop:hadoop-aws:3.4.1,com.amazonaws:aws-java-sdk-bundle:1.11.1026") \
    .getOrCreate()
    
# df = spark.read.parquet("s3a://ml-user-events/user-events/event_date_time=2025-12-25 19-47-56/part-00000-7b04519f-6b53-4f8c-9ed3-9804541e1c89.c000.snappy.parquet")
df = spark.read.parquet("s3a://ml-user-events/user-events/event_date_time=2025-12-25 19-47-56/")

print("Schema: ")
df.printSchema()

print("Data: ")
df.show(truncate=False)

print(f"Total records: {df.count()}")