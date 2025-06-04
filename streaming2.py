from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from streaming import logs

spark = SparkSession.builder \
    .appName("StreamLogsToFS") \
    .getOrCreate()

schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("ip", StringType(), True),
    StructField("user_agent", StringType(), True)
])

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "logs_topic") \
    .load()

metrics = logs \
    .withColumn("date", to_date("timestamp")) \
    .groupBy("ip", "user_agent", "date") \
    .agg(count("*").alias("connection_count"))

metrics_by_ip = df.groupBy("ip").agg(count("*").alias("connection_count"))
query_ip = metrics_by_ip.writeStream \
    .outputMode("complete") \
    .format("json") \
    .option("path", "data/speed_layer/metrics_by_ip") \
    .option("checkpointLocation", "data/checkpoint/metrics_by_ip") \
    .trigger(processingTime="1 minute") \
    .start()

metrics_by_agent = df.groupBy("user_agent").agg(count("*").alias("connection_count"))
query_agent = metrics_by_agent.writeStream \
    .outputMode("complete") \
    .format("json") \
    .option("path", "data/speed_layer/metrics_by_agent") \
    .option("checkpointLocation", "data/checkpoint/metrics_by_agent") \
    .trigger(processingTime="1 minute") \
    .start()

metrics_by_day = df.groupBy("date").agg(count("*").alias("connection_count"))
query_day = metrics_by_day.writeStream \
    .outputMode("complete") \
    .format("json") \
    .option("path", "data/speed_layer/metrics_by_day") \
    .option("checkpointLocation", "data/checkpoint/metrics_by_day") \
    .trigger(processingTime="1 minute") \
    .start()

query_ip.awaitTermination()
query_agent.awaitTermination()
query_day.awaitTermination()

