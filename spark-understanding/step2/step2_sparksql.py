# RDD vs DataFrame vs Spark SQL
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Step 2 - Spark SQL") \
    .master("local[*]") \
    .getOrCreate()
    
df = spark.createDataFrame(
    [
        (1, "click"), (2, "view"), (3, "click")
    ],
    ["user_id", "event_type"]
)

df.select("user_id").filter(df.event_type == "click").explain(True)