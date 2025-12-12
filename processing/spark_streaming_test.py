# processing/spark_streaming_test.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
# from monitoring.logging_config import setup_logger
# logger=setup_logger()
# Create Spark session
spark = SparkSession.builder \
    .appName("NiftyStreamTest") \
    .master("spark://spark-master:7077") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoint") \
    .config("spark.kafka.producer.bootstrap.servers", "kafka:29092") \
    .config("spark.kafka.consumer.bootstrap.servers", "kafka:29092") \
    .config("spark.streaming.kafka.consumer.cache.enabled", "false") \
    .getOrCreate()

print("*****************spark session created successfully********************")

df = spark.readStream\
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "raw-stock") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .option("kafkaConsumer.pollTimeoutMs", "5000") \
    .load()

# Print schema
print("Kafka DataFrame Schema:")
df.printSchema()

string_df = df.selectExpr(
    "CAST(key AS STRING) as key",
    "CAST(value AS STRING) as value",
    "topic",
    "partition",
    "offset",
    "timestamp as kafka_timestamp"
)

schema = StructType([
    StructField("symbol", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("open", FloatType(), True),
    StructField("high", FloatType(), True),
    StructField("low", FloatType(), True),
    StructField("close", FloatType(), True),
    StructField("volume", LongType(), True),
    StructField("sector", StringType(), True),
    StructField("source", StringType(), True),
    StructField("ingestion_time", StringType(), True)
])

# Parse JSON using the schema
parsed_df = string_df.select(
    from_json(col("value"), schema).alias("data"),
    col("topic"),
    col("partition"),
    col("offset"),
    col("kafka_timestamp")
).select("data.*", "topic", "partition", "offset", "kafka_timestamp")
print("parsed df schema: ")
parsed_df.printSchema()

# Show data (for testing)
query = parsed_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()