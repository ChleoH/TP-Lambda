from delta import configure_spark_with_delta_pip  
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType

builder = SparkSession.builder.appName("DeltaLakeBatch")\
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("ip", StringType(), True),
    StructField("user_agent", StringType(), True)
])

df = spark.read.schema(schema).json("data/streaming_logs")
df = df.withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss")) \
       .withColumn("date", to_date("timestamp"))

metrics_by_ip = df.groupBy("ip").agg(count("*").alias("connection_count"))
metrics_by_ip.write.format("delta") \
    .mode("overwrite") \
    .save("data/delta/metrics_by_ip")

metrics_by_agent = df.groupBy("user_agent").agg(count("*").alias("connection_count"))
metrics_by_agent.write.format("delta") \
    .mode("overwrite") \
    .save("data/delta/metrics_by_agent")

metrics_by_day = df.groupBy("date").agg(count("*").alias("connection_count"))
metrics_by_day.write.format("delta") \
    .mode("overwrite") \
    .partitionBy("date") \
    .save("data/delta/metrics_by_day")
