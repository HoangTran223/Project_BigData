"""
Spark Structured Streaming job to process air quality data from Kafka
and write to MinIO in Bronze/Silver/Gold layers.
"""

import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, year, month, dayofmonth,
    when, isnan, isnull, max as spark_max, collect_list, struct,
    window, current_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType,
    TimestampType, IntegerType, ArrayType
)

# Configuration from environment
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "openaq-raw-measurements")
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin123")
MINIO_BUCKET = os.environ.get("MINIO_BUCKET", "air-quality-data")

# MinIO S3 path
S3_PATH = f"s3a://{MINIO_BUCKET}"

# Kafka message schema
KAFKA_MESSAGE_SCHEMA = StructType([
    StructField("datetime", StringType(), True),
    StructField("location_id", LongType(), True),
    StructField("location_name", StringType(), True),
    StructField("country", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("parameter", StringType(), True),
    StructField("value", DoubleType(), True),
    StructField("value_standard", DoubleType(), True),
    StructField("unit", StringType(), True),
    StructField("aqi", DoubleType(), True),
    StructField("aqi_category", StringType(), True),
    StructField("ingestion_timestamp", StringType(), True),
])


def create_spark_session():
    """Create Spark session with MinIO S3 configuration."""
    spark = SparkSession.builder \
        .appName("AirQualityStreamProcessor") \
        .config("spark.sql.streaming.checkpointLocation", f"{S3_PATH}/checkpoints") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.minPartitionNum", "1") \
        .config("spark.sql.adaptive.coalescePartitions.initialPartitionNum", "10") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .config("spark.sql.shuffle.partitions", "10") \
        .config("spark.sql.adaptive.shuffle.targetPostShuffleInputSize", "64MB") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark


def process_bronze_layer(spark, kafka_df):
    """Process Bronze layer: Save raw data as-is."""
    print("Setting up Bronze layer processing...")
    
    # Parse JSON and extract fields
    bronze_df = kafka_df.select(
        from_json(col("value").cast("string"), KAFKA_MESSAGE_SCHEMA).alias("data")
    ).select(
        to_timestamp(col("data.datetime")).alias("datetime"),
        col("data.location_id").alias("location_id"),
        col("data.location_name").alias("location_name"),
        col("data.country").alias("country"),
        col("data.latitude").alias("latitude"),
        col("data.longitude").alias("longitude"),
        col("data.parameter").alias("parameter"),
        col("data.value").alias("value"),
        col("data.value_standard").alias("value_standard"),
        col("data.unit").alias("unit"),
        col("data.aqi").alias("aqi"),
        col("data.aqi_category").alias("aqi_category"),
        to_timestamp(col("data.ingestion_timestamp")).alias("ingestion_timestamp")
    ).filter(
        col("datetime").isNotNull() & 
        col("location_id").isNotNull() &
        col("parameter").isNotNull()
    )
    
    # Add partition columns
    bronze_df = bronze_df.withColumn("year", year(col("datetime"))) \
        .withColumn("month", month(col("datetime"))) \
        .withColumn("day", dayofmonth(col("datetime")))
    
    # Write to MinIO
    bronze_query = bronze_df.writeStream \
        .format("parquet") \
        .outputMode("append") \
        .option("path", f"{S3_PATH}/bronze") \
        .option("checkpointLocation", f"{S3_PATH}/checkpoints/bronze") \
        .partitionBy("year", "month", "day") \
        .trigger(processingTime="10 seconds") \
        .start()
    
    print("Bronze layer streaming started")
    return bronze_query


def process_silver_layer(spark, kafka_df):
    """Process Silver layer: Clean and standardize data."""
    print("Setting up Silver layer processing...")
    
    # Parse JSON and extract fields
    silver_df = kafka_df.select(
        from_json(col("value").cast("string"), KAFKA_MESSAGE_SCHEMA).alias("data")
    ).select(
        to_timestamp(col("data.datetime")).alias("datetime"),
        col("data.location_id").alias("location_id"),
        col("data.location_name").alias("location_name"),
        col("data.country").alias("country"),
        col("data.latitude").alias("latitude"),
        col("data.longitude").alias("longitude"),
        col("data.parameter").alias("parameter"),
        col("data.value_standard").alias("value"),  # Use value_standard only
        col("data.aqi").alias("aqi"),
        col("data.aqi_category").alias("aqi_category"),
        current_timestamp().alias("processing_timestamp")
    ).filter(
        col("datetime").isNotNull() &
        col("location_id").isNotNull() &
        col("parameter").isNotNull() &
        col("value").isNotNull() &
        (col("value") >= 0) &  # Only valid values
        ~isnan(col("value"))
    )
    
    # Add partition columns
    silver_df = silver_df.withColumn("year", year(col("datetime"))) \
        .withColumn("month", month(col("datetime"))) \
        .withColumn("day", dayofmonth(col("datetime")))
    
    # Write to MinIO
    silver_query = silver_df.writeStream \
        .format("parquet") \
        .outputMode("append") \
        .option("path", f"{S3_PATH}/silver") \
        .option("checkpointLocation", f"{S3_PATH}/checkpoints/silver") \
        .partitionBy("year", "month", "day") \
        .trigger(processingTime="10 seconds") \
        .start()
    
    print("Silver layer streaming started")
    return silver_query


def process_gold_layer(spark, kafka_df):
    """Process Gold layer: Aggregate by location and hour."""
    print("Setting up Gold layer processing...")
    
    # Parse JSON and extract fields
    from pyspark.sql.functions import hour, date_trunc
    
    gold_df = kafka_df.select(
        from_json(col("value").cast("string"), KAFKA_MESSAGE_SCHEMA).alias("data")
    ).select(
        to_timestamp(col("data.datetime")).alias("datetime"),
        col("data.location_id").alias("location_id"),
        col("data.location_name").alias("location_name"),
        col("data.country").alias("country"),
        col("data.latitude").alias("latitude"),
        col("data.longitude").alias("longitude"),
        col("data.parameter").alias("parameter"),
        col("data.value_standard").alias("value"),
        col("data.aqi").alias("aqi"),
        col("data.aqi_category").alias("aqi_category")
    ).filter(
        col("datetime").isNotNull() &
        col("location_id").isNotNull() &
        col("parameter").isNotNull() &
        col("value").isNotNull() &
        (col("value") >= 0) &
        ~isnan(col("value")) &
        col("aqi").isNotNull()
    )
    
    # Add watermark BEFORE aggregation (required by Spark for append mode with aggregation)
    # datetime is already TimestampType from to_timestamp above
    gold_df = gold_df.withWatermark("datetime", "1 hour")
    
    # Group by location and hour, aggregate
    gold_aggregated = gold_df \
        .withColumn("hour_datetime", date_trunc("hour", col("datetime"))) \
        .groupBy(
            col("hour_datetime").alias("datetime"),
            col("location_id"),
            col("location_name"),
            col("country"),
            col("latitude"),
            col("longitude")
        ) \
        .agg(
            spark_max("aqi").alias("aqi"),
            collect_list("parameter").alias("parameters"),
            collect_list("value").alias("values"),
            spark_max("aqi_category").alias("aqi_category")  # Take max category
        )
    
    # Add partition columns
    gold_aggregated = gold_aggregated.withColumn("year", year(col("datetime"))) \
        .withColumn("month", month(col("datetime"))) \
        .withColumn("day", dayofmonth(col("datetime")))
    
    # Write to MinIO
    gold_query = gold_aggregated.writeStream \
        .format("parquet") \
        .outputMode("append") \
        .option("path", f"{S3_PATH}/gold") \
        .option("checkpointLocation", f"{S3_PATH}/checkpoints/gold") \
        .partitionBy("year", "month", "day") \
        .trigger(processingTime="1 minute") \
        .start()
    
    print("Gold layer streaming started")
    return gold_query


def main():
    """Main function to start Spark streaming."""
    print("Initializing Spark session...")
    spark = create_spark_session()
    
    print(f"Reading from Kafka: {KAFKA_BOOTSTRAP_SERVERS}, topic: {KAFKA_TOPIC}")
    
    # Read from Kafka
    kafka_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()
    
    # Start all three streaming queries
    print("Starting all streaming queries...")
    bronze_query = process_bronze_layer(spark, kafka_df)
    silver_query = process_silver_layer(spark, kafka_df)
    gold_query = process_gold_layer(spark, kafka_df)
    
    print("All streaming queries started. Waiting for termination...")
    
    # Wait for all queries
    import threading
    
    def wait_for_query(query, name):
        try:
            query.awaitTermination()
        except Exception as e:
            print(f"Error in {name} query: {e}")
    
    threads = [
        threading.Thread(target=wait_for_query, args=(bronze_query, "Bronze")),
        threading.Thread(target=wait_for_query, args=(silver_query, "Silver")),
        threading.Thread(target=wait_for_query, args=(gold_query, "Gold"))
    ]
    
    for thread in threads:
        thread.start()
    
    for thread in threads:
        thread.join()
    
    print("All streaming queries stopped.")


if __name__ == "__main__":
    main()

