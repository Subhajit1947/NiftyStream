# processing/spark_streaming_test.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os
from dotenv import load_dotenv

# from monitoring.logging_config import setup_logger
# logger=setup_logger()
# Create Spark session
from business_indicator import claculate_business_indicator
load_dotenv()
spark = SparkSession.builder \
    .appName("NiftyStreamTest") \
    .master("spark://spark-master:7077") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.5.0")\
    .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoint") \
    .config("spark.kafka.producer.bootstrap.servers", "kafka:29092") \
    .config("spark.kafka.consumer.bootstrap.servers", "kafka:29092") \
    .config("spark.streaming.kafka.consumer.cache.enabled", "false") \
    .getOrCreate()

print("*****************spark session created successfully********************")
postgres_url = os.getenv("POSTGRES_URL")
postgres_properties = {
    "user":os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
    "driver": "org.postgresql.Driver"
}

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

critical_cols = ["open", "high", "low", "close"]
df = parsed_df.dropna(subset=critical_cols)

df = df.withColumn(
    "volume",
    when(col("volume").isNull(), lit(0)).otherwise(col("volume"))
)
string_cols = ["sector", "source", "topic"]
df = df.dropna(subset=string_cols)
df = df.withColumn(
    "ingestion_time",
    when(
        col("ingestion_time").isNotNull(),
        to_timestamp(col("ingestion_time"))
    ).otherwise(
        current_timestamp()
    )
)



def write_to_both(batch_df, batch_id):
    try:
        print(f"\n{'='*60}")
        print(f"Processing Batch {batch_id}")
        print(f"{'='*60}")
        
        print(f"Batch size: {batch_df.count()} records")
        
        if batch_df.count() == 0:
            print("Empty batch, skipping...")
            return
        result = claculate_business_indicator(batch_df)
        print(f"2. After calculations: {result.count()} records")
        
        # Show sample in console
        print("3. Sample data:")
        result.select(
            "symbol", "timestamp", "close", 
            "vwap", "price_spike",
            "volume_surge", "rsi"
        ).show(5, truncate=False)
        
        result_stock_metrics = result.select(
            col("symbol"),
            to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss").alias("timestamp"),
            col("open").alias("open_price"),
            col("high").alias("high_price"),
            col("low").alias("low_price"),
            col("close").alias("close_price"),
            col("volume"),
            col("price_change"),
            col("percent_change"),
            col("sma_5"),
            col("sma_20"),
            col("volume_ratio"),
            col("rsi"),
            col("vwap"),
            col("volume_surge").alias("is_volume_surge"),
            col("price_spike").alias("is_price_spike"),
            col("rsi_oversold").alias("is_rsi_oversold"),
            col("rsi_overbought").alias("is_rsi_overbought"),
            col("processing_time")
        )
        print("\n4. Writing to stock_metrics table...")
        result_stock_metrics.write \
            .mode("append") \
            .jdbc(url=postgres_url, 
                  table=os.getenv("STOCK_TABLE"), 
                  properties=postgres_properties)
        
        print(f"Written {result_stock_metrics.count()} records to PostgreSQL")
        alerts = result.filter(
            col("price_spike") | 
            col("volume_surge") | 
            col("rsi_oversold") | 
            col("rsi_overbought")
        )
        alerts_df = alerts.select(
            col("symbol"),
            when(col("price_spike"), "PRICE_SPIKE")
            .when(col("volume_surge"), "VOLUME_SURGE")
            .when(col("rsi_oversold"), "RSI_OVERSOLD")
            .when(col("rsi_overbought"), "RSI_OVERBOUGHT")
            .otherwise("UNKNOWN").alias("alert_type"),
            to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss").alias("timestamp"),
            concat(
                col("symbol"), lit(" - "),
                when(col("price_spike"), 
                    concat(lit("Price spike: "), round(col("minute_percent_change"), 2), lit("%")))
                .when(col("volume_surge"),
                    concat(lit("Volume surge: "), round(col("volume_ratio"), 2), lit("x average")))
                .when(col("rsi_oversold"),
                    concat(lit("RSI oversold: "), round(col("rsi"), 2)))
                .when(col("rsi_overbought"),
                    concat(lit("RSI overbought: "), round(col("rsi"), 2)))
                .otherwise("Alert").alias("message")
            ).alias("message"),
            col("close").alias("close_price"),
            col("volume"),
            col("rsi"),
            lit(True).alias("is_active"),
            current_timestamp().alias("created_at")
        )
        if alerts_df.count() > 0:
            print(f"Found {alerts_df.count()} alerts")
            
            # Show alerts in console
            print("\n Alerts found:")
            alerts_df.select(
                "symbol", "alert_type", "timestamp",
                "close_price", "rsi"
            ).show(10, truncate=False)
            
            # Write to alerts table
            print("\n   Writing to alerts table...")
            alerts_df.write \
                .mode("append") \
                .jdbc(url=postgres_url, 
                      table=os.getenv("ALERT_TABLE"), 
                      properties=postgres_properties)
            
            print(f"Written {alerts_df.count()} alerts to alerts table")
        else:
            print("No alerts found in this batch")

        
    except Exception as e:
        print(f"Error processing batch {batch_id}: {e}")
        batch_df.select("symbol", "timestamp", "close").show(5, truncate=False)
    



query = df.writeStream \
    .outputMode("append") \
    .foreachBatch(write_to_both) \
    .start()

query.awaitTermination()


# Show data (for testing)
# query = df.writeStream \
#     .outputMode("complete") \
#     .format("console") \
#     .start()

# query.awaitTermination()