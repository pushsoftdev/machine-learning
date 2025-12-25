from datetime import timedelta
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import col,window,sum,count,from_json,to_timestamp, date_format
import mysql.connector

# Create the Spark Session
spark = SparkSession \
    .builder \
    .appName('Real time website analytics') \
    .config('spark.sql.streaming.forceDeleteTempCheckpointLocation', 'true') \
    .config('spark.hadoop.fs.s3a.access.key', os.getenv("AWS_ACCESS_KEY")) \
    .config('spark.hadoop.fs.s3a.secret.key', os.getenv("AWS_SECRET_KEY")) \
    .config('spark.hadoop.fs.s3a.endpoint', "s3.amazonaws.com") \
    .config('spark.hadoop.fs.s3a.endpoint.region', os.getenv("AWS_DEFAULT_REGION")) \
    .config('spark.hadoop.fs.s3a.impl', "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config('spark.jars.packages', "org.apache.hadoop:hadoop-aws:3.4.1,com.amazonaws:aws-java-sdk-bundle:1.11.1026,org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1") \
    .getOrCreate()
    
print("Spark Version: ", spark.version)

# Declare the Json schema for the payload
payload_schema = StructType(
    [
        StructField("user_id", IntegerType()),
        StructField("event_time", StringType()),
        StructField("event_type", StringType()),
        StructField("value", IntegerType())
    ]
)

# Get the raw dataframe from the Kafka stream
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "user_events") \
    .option("startingOffsets", "latest") \
    .option("maxOffsetsPerTrigger", 500) \
    .load()

# Parse the raw dataframe to object using the schema
parsed_df = raw_df \
    .select(
        from_json(col("value").cast("string"), payload_schema).alias("data"),
        col("partition"),
        col("offset")
    ) \
    .select(
        col("data.user_id").alias("user_id"),
        col("data.event_type").alias("event_type"),
        col("data.value").alias("value"),
        to_timestamp(col("data.event_time"), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("event_time"),
        col("partition"),
        col("offset")
    ) \
    .withColumn("event_date_time", date_format(col("event_time"), "yyyy-MM-dd HH-mm-ss"))
    
s3_query = parsed_df.writeStream \
    .format("parquet") \
    .outputMode("append") \
    .option("path", "s3a://ml-user-events/user-events/") \
    .option("checkpointLocation", "/tmp/checkpoints/s3_raw_events") \
    .partitionBy("event_date_time") \
    .trigger(processingTime="10 seconds") \
    .start()

# Apply watermark to eliminate the late records (records older than 30s)
watermarked_df = parsed_df.withWatermark("event_time", "30 seconds")

# Aggregate the records (Group by user_id, event_type and window)
agg_df = watermarked_df \
    .groupBy(
        window(col("event_time"), "1 minute"), col("user_id"), col("event_type")
    ) \
    .agg(
        sum("value").alias("total_value"),
        count("*").alias("events_count")
    ) \
    .selectExpr(
        "window.start as window_start",
        "window.end as window_end",
        "user_id",
        "event_type",
        "total_value",
        "events_count"
    )
    
mysql_config = {
    "host": "localhost",
    "port": "3306",
    "database": "kafka_demo",
    "user": "spark",
    "password": ""
}
    
def write_to_mysql(batch_df, batch_id):
    rows = batch_df.collect()
    
    connection = mysql.connector.connect(**mysql_config)
    cursor = connection.cursor()
    
    sql = '''
        INSERT INTO user_event_aggregates(user_id, event_type, window_start, window_end,total_value,event_count)
        VALUES(%s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            total_value=VALUES(total_value),
            event_count=VALUES(event_count)
    '''
    
    data = []
    for row in rows:
        data.append(
            (
                row.user_id,
                row.event_type,
                row.window_start,
                row.window_end,
                row.total_value,
                row.events_count                
            )
        )
    
    cursor.executemany(sql, data)
    connection.commit()
    cursor.close()
    connection.close() 

# Write the stream to a MySQL database
agg_query = agg_df.writeStream \
    .outputMode("update") \
    .foreachBatch(write_to_mysql) \
    .option("checkpointLocation", "/tmp/checkpoints/aggregated_query") \
    .start()
    
agg_query.awaitTermination()
s3_query.awaitTermination()