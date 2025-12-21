"""
Spark Structured Streaming job to process air quality data from Kafka
and store in MinIO (S3) in Bronze/Silver/Gold layers.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, struct, to_timestamp, 
    window, max as spark_max, collect_list, 
    current_timestamp, year, month, dayofmonth
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, 
    IntegerType, TimestampType, ArrayType
)
import os
import threading

# Configuration - All services run in Docker
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "openaq-raw-measurements")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin123")
MINIO_BUCKET = os.environ.get("MINIO_BUCKET", "air-quality-data")

# Schema for incoming Kafka messages
MEASUREMENT_SCHEMA = StructType([
    StructField("datetime", StringType(), True),
    StructField("location_id", IntegerType(), True),
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
    builder = SparkSession.builder \
        .appName("OpenAQStreamProcessor") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.sql.streaming.checkpointLocation", f"s3a://{MINIO_BUCKET}/checkpoints/")
    
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def process_bronze_layer(df, epoch_id):
    """Write raw data to Bronze layer (Parquet format)."""
    if df.count() > 0:
        # Convert datetime and add partition columns
        df_with_partitions = df.withColumn("datetime", to_timestamp(col("datetime"))) \
                              .withColumn("year", year(col("datetime"))) \
                              .withColumn("month", month(col("datetime"))) \
                              .withColumn("day", dayofmonth(col("datetime")))
        
        # Write to Bronze layer - Spark will auto-create partition directories
        bronze_path = f"s3a://{MINIO_BUCKET}/bronze/"
        
        df_with_partitions.write \
            .mode("append") \
            .partitionBy("year", "month", "day") \
            .parquet(bronze_path)
        
        print(f"[Bronze] Wrote {df.count()} records to {bronze_path}")


def process_silver_layer(df, epoch_id):
    """Process and clean data for Silver layer."""
    if df.count() > 0:
        # Clean and normalize data
        silver_df = df.filter(col("value").isNotNull()) \
                     .filter(col("value") >= 0) \
                     .withColumn("datetime", to_timestamp(col("datetime"))) \
                     .select(
                         col("datetime"),
                         col("location_id"),
                         col("location_name"),
                         col("country"),
                         col("latitude"),
                         col("longitude"),
                         col("parameter"),
                         col("value_standard").alias("value"),
                         col("unit"),
                         col("aqi"),
                         col("aqi_category"),
                         current_timestamp().alias("processing_timestamp")
                     )
        
        # Add partition columns
        silver_df_with_partitions = silver_df.withColumn("year", year(col("datetime"))) \
                                            .withColumn("month", month(col("datetime"))) \
                                            .withColumn("day", dayofmonth(col("datetime")))
        
        # Write to Silver layer - Spark will auto-create partition directories
        silver_path = f"s3a://{MINIO_BUCKET}/silver/"
        
        silver_df_with_partitions.write \
            .mode("append") \
            .partitionBy("year", "month", "day") \
            .parquet(silver_path)
        
        print(f"[Silver] Wrote {silver_df.count()} records to {silver_path}")


def process_gold_layer(df, epoch_id):
    """Aggregate data for Gold layer (hourly AQI)."""
    if df.count() > 0:
        # Convert datetime and create window
        df_with_time = df.withColumn("datetime", to_timestamp(col("datetime")))
        
        # Group by location and hour, calculate max AQI
        gold_df = df_with_time \
            .filter(col("aqi").isNotNull()) \
            .groupBy(
                window(col("datetime"), "1 hour").alias("time_window"),
                col("location_id"),
                col("location_name"),
                col("country"),
                col("latitude"),
                col("longitude")
            ) \
            .agg(
                spark_max("aqi").alias("aqi"),
                spark_max("aqi_category").alias("aqi_category"),
                collect_list("parameter").alias("parameters"),
                collect_list("value_standard").alias("values")
            ) \
            .withColumn("datetime", col("time_window.start")) \
            .drop("time_window") \
            .withColumn("aggregation_timestamp", current_timestamp())
        
        if gold_df.count() > 0:
            # Add partition columns
            gold_df_with_partitions = gold_df.withColumn("year", year(col("datetime"))) \
                                            .withColumn("month", month(col("datetime"))) \
                                            .withColumn("day", dayofmonth(col("datetime")))
            
            # Write to Gold layer - Spark will auto-create partition directories
            gold_path = f"s3a://{MINIO_BUCKET}/gold/"
            
            gold_df_with_partitions.write \
                .mode("append") \
                .partitionBy("year", "month", "day") \
                .parquet(gold_path)
            
            print(f"[Gold] Wrote {gold_df.count()} aggregated records to {gold_path}")


def main():
    """Main streaming processing function."""
    spark = create_spark_session()
    
    print("Starting Spark Structured Streaming job...")
    print(f"Reading from Kafka: {KAFKA_BOOTSTRAP_SERVERS}/{KAFKA_TOPIC}")
    print(f"Writing to MinIO: {MINIO_ENDPOINT}/{MINIO_BUCKET}")
    
    # Read from Kafka
    kafka_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()
    
    # Parse JSON from Kafka
    parsed_df = kafka_df.select(
        from_json(col("value").cast("string"), MEASUREMENT_SCHEMA).alias("data")
    ).select("data.*")
    
    # Process in three layers
    # Bronze: Raw data
    bronze_query = parsed_df.writeStream \
        .foreachBatch(process_bronze_layer) \
        .outputMode("append") \
        .trigger(processingTime='10 seconds') \
        .start()
    
    # Silver: Cleaned data
    silver_query = parsed_df.writeStream \
        .foreachBatch(process_silver_layer) \
        .outputMode("append") \
        .trigger(processingTime='10 seconds') \
        .start()
    
    # Gold: Aggregated hourly AQI
    gold_query = parsed_df.writeStream \
        .foreachBatch(process_gold_layer) \
        .outputMode("append") \
        .trigger(processingTime='1 minute') \
        .start()
    
    print("Streaming queries started. Waiting for data...")
    
    # Wait for termination - use threading to await all queries in parallel
    def await_query(query, name):
        try:
            query.awaitTermination()
        except Exception as e:
            print(f"{name} query error: {e}")
    
    # Start threads for each query
    bronze_thread = threading.Thread(target=await_query, args=(bronze_query, "Bronze"), daemon=True)
    silver_thread = threading.Thread(target=await_query, args=(silver_query, "Silver"), daemon=True)
    gold_thread = threading.Thread(target=await_query, args=(gold_query, "Gold"), daemon=True)
    
    bronze_thread.start()
    silver_thread.start()
    gold_thread.start()
    
    # Wait for all threads (main thread will block here)
    bronze_thread.join()
    silver_thread.join()
    gold_thread.join()


if __name__ == "__main__":
    main()
