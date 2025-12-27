from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Step 2 - Final") \
    .master("local[*]") \
    .getOrCreate()
    
data = [
    ("u1", "click"),
    ("u2", "view"),
    ("u3", "click"),
    ("u4", "view")
]

df = spark.createDataFrame(data, ["user_id", "event_type"])

# DataFrame API
print("JP: DataFrame API")
df.groupBy("event_type").count().show()

#SQL API
df.createOrReplaceTempView("events")
print("JP: SQL API")
spark.sql(""" 
          select event_type, count(*) as cnt
          from events
          group by event_type
""")

# RDD Version
rdd = df.rdd
print("JP: RDD Version")
print(
    rdd.map(lambda r: r.event_type)
)