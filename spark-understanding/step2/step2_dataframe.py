# RDD vs DataFrame vs Spark SQL
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Step 2 - DataFrame") \
    .master("local[*]") \
    .getOrCreate()
    
df = spark.createDataFrame(
    [
        (1, "click"), (2, "view"), (3, "click")
    ],
    ["user_id", "event_type"]
)

df.filter(df.event_type == "click").show()