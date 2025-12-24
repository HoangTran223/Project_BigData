"""
Spark Batch Processing job for ML Training Data Pipeline
Pipeline: Kafka → Bronze → Silver → Gold (sequential, not parallel)
"""

import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, year, month, dayofmonth,
    when, isnan, isnull, max as spark_max, collect_list,
    date_trunc
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType,
    TimestampType
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
    import multiprocessing
    num_cores = multiprocessing.cpu_count()
    shuffle_dir = f"{S3_PATH}/spark-shuffle"
    
    spark = SparkSession.builder \
        .appName("AirQualityBatchProcessor") \
        .master(f"local[{num_cores}]") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.local.dir", shuffle_dir) \
        .config("spark.executor.cores", str(num_cores)) \
        .config("spark.executor.memory", "8g") \
        .config("spark.driver.memory", "4g") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.minPartitionNum", "1") \
        .config("spark.sql.adaptive.coalescePartitions.initialPartitionNum", str(num_cores * 2)) \
        .config("spark.sql.shuffle.partitions", str(num_cores * 2)) \
        .config("spark.sql.adaptive.shuffle.targetPostShuffleInputSize", "128MB") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    print(f"[Spark] Initialized with {num_cores} cores (local[{num_cores}] mode)")
    return spark


def process_kafka_to_bronze(spark):
    """Job 1: Read from Kafka → Write to Bronze (Raw Data)"""
    print("[Job 1] Processing Kafka → Bronze...")
    
    # Read from Kafka
    kafka_df = spark \
        .read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .option("endingOffsets", "latest") \
        .load()
    
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
    
    # Write to MinIO (overwrite mode for batch processing)
    bronze_path = f"{S3_PATH}/bronze"
    print(f"[Job 1] Writing to {bronze_path}...")
    bronze_df.write \
        .mode("overwrite") \
        .format("parquet") \
        .partitionBy("year", "month", "day") \
        .save(bronze_path)
    
    count = bronze_df.count()
    print(f"[Job 1] ✓ Completed: {count} records written to Bronze")
    return bronze_path


def process_bronze_to_silver(spark):
    """Job 2: Read from Bronze → Write to Silver (Cleaned Data)"""
    print("[Job 2] Processing Bronze → Silver...")
    
    # Read from Bronze
    bronze_path = f"{S3_PATH}/bronze"
    bronze_df = spark.read.parquet(bronze_path)
    
    # Clean and standardize
    silver_df = bronze_df.select(
        col("datetime"),
        col("location_id"),
        col("location_name"),
        col("country"),
        col("latitude"),
        col("longitude"),
        col("parameter"),
        col("value_standard").alias("value"),  # Use value_standard only
        col("aqi"),
        col("aqi_category"),
        col("year"),
        col("month"),
        col("day")
    ).filter(
        col("datetime").isNotNull() &
        col("location_id").isNotNull() &
        col("parameter").isNotNull() &
        col("value").isNotNull() &
        (col("value") >= 0) &  # Only valid values
        ~isnan(col("value"))
    )
    
    # Write to MinIO
    silver_path = f"{S3_PATH}/silver"
    print(f"[Job 2] Writing to {silver_path}...")
    silver_df.write \
        .mode("overwrite") \
        .format("parquet") \
        .partitionBy("year", "month", "day") \
        .save(silver_path)
    
    count = silver_df.count()
    print(f"[Job 2] ✓ Completed: {count} records written to Silver")
    return silver_path


def process_silver_to_gold(spark):
    """Job 3: Read from Silver → Write to Gold (Aggregated by Hour)"""
    print("[Job 3] Processing Silver → Gold...")
    
    # Read from Silver
    silver_path = f"{S3_PATH}/silver"
    silver_df = spark.read.parquet(silver_path)
    
    # Aggregate by location and hour
    gold_df = silver_df \
        .withColumn("hour_datetime", date_trunc("hour", col("datetime"))) \
        .groupBy(
            col("hour_datetime").alias("datetime"),
            col("location_id"),
            col("location_name"),
            col("country"),
            col("latitude"),
            col("longitude"),
            col("year"),
            col("month"),
            col("day")
        ) \
        .agg(
            spark_max("aqi").alias("aqi"),
            collect_list("parameter").alias("parameters"),
            collect_list("value").alias("values"),
            spark_max("aqi_category").alias("aqi_category")
        )
    
    # Write to MinIO
    gold_path = f"{S3_PATH}/gold"
    print(f"[Job 3] Writing to {gold_path}...")
    gold_df.write \
        .mode("overwrite") \
        .format("parquet") \
        .partitionBy("year", "month", "day") \
        .save(gold_path)
    
    count = gold_df.count()
    print(f"[Job 3] ✓ Completed: {count} records written to Gold")
    return gold_path


def main():
    """Main function: Run batch processing pipeline sequentially."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Batch processing pipeline for ML training data")
    parser.add_argument("--layer", choices=["bronze", "silver", "gold", "all"], 
                       default="all", help="Which layer to process")
    args = parser.parse_args()
    
    print("=" * 60)
    print("Spark Batch Processing Pipeline for ML Training")
    print("=" * 60)
    
    spark = create_spark_session()
    
    try:
        if args.layer == "bronze" or args.layer == "all":
            process_kafka_to_bronze(spark)
        
        if args.layer == "silver" or args.layer == "all":
            process_bronze_to_silver(spark)
        
        if args.layer == "gold" or args.layer == "all":
            process_silver_to_gold(spark)
        
        print("=" * 60)
        print("✓ All batch processing jobs completed successfully!")
        print("=" * 60)
        
    except Exception as e:
        print(f"✗ Error in batch processing: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

