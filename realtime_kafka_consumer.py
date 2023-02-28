from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName('SP500 Streaming') \
    .master('spark://master:7078') \
    .config('spark.executor.memory', '4g') \
    .getOrCreate()



schema = StructType([
    StructField("Symbol", StringType()),
    StructField("Price", DoubleType()),
    StructField("Time", StringType())
])

# 读取Kafka主题中的数据流 Read Stream from Kafka Topic
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9094") \
    .option("subscribe", "sp500") \
    .option("startingOffsets", "earliest") \
    .load() \
    .select(from_json(col("value").cast("string"), schema).alias("data")) \
    .select("data.Symbol", "data.Price", "data.Time")

# Compute the average and volatility
agg_df = df \
    .withWatermark("Time", "1 minute") \
    .groupBy(window("Time", "1 minute"), "Symbol") \
    .agg(avg("Price").alias("avg_price"), stddev("Price").alias("volatility"))


query = agg_df \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()
