from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pyspark
from streaming import logs

spark = SparkSession.builder \
    .appName("StreamLogsToFS") \
    .getOrCreate()

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "logs_topic") \
    .load()

metrics = logs \
    .withColumn("date", to_date("timestamp")) \
    .groupBy("ip", "user_agent", "date") \
    .agg(count("*").alias("connection_count"))

query_metrics = metrics.writeStream \
    .format("json") \
    .option("path", "data/speed_layer/") \
    .option("checkpointLocation", "data/checkpoint_metrics/") \
    .trigger(processingTime="1 minute") \
    .partitionBy("date")\
    .trigger(processingTime="1minute")\
    .start()

query_metrics.awaitTermination()
