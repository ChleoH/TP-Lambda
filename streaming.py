from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pyspark

spark = SparkSession.builder \
    .appName("StreamLogsToFS") \
    .getOrCreate()

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "logs_topic") \
    .load()

logs = df.selectExpr("CAST(value AS STRING)") \
    .select(split(col("value"), ",").alias("parts")) \
    .select(
        col("parts")[0].alias("timestamp"),
        col("parts")[1].alias("ip"),
        col("parts")[2].alias("user_agent")
    )

query_raw = logs.writeStream \
    .format("json") \
    .option("path", "data/streaming_logs/") \
    .option("checkpointLocation", "data/checkpoint/") \
    .trigger(processingTime="1 minute") \
    .start()

query_raw.awaitTermination()