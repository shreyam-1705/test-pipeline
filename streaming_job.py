import os
import time
import threading
import sys
import traceback
from huggingface_hub import create_bucket, sync_bucket
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark import SparkFiles

# --- 1. THE SYNCER THREAD ---
def sync_to_huggingface():
    namespace = os.getenv("HF_NAMESPACE")
    bucket_name = os.getenv("HF_BUCKET_NAME")
    token = os.getenv("HF_TOKEN")
    bucket_id = f"{namespace}/{bucket_name}"

    # Debug: confirm env vars actually made it into this process
    print(f"[sync] HF_NAMESPACE={namespace} HF_BUCKET_NAME={bucket_name} "
          f"HF_TOKEN={'set' if token else 'MISSING'}", flush=True)

    # Retry bucket creation instead of letting one failure kill the thread forever
    while True:
        try:
            create_bucket(bucket_id, token=token, exist_ok=True)
            print(f"[sync] Bucket ready: hf://buckets/{bucket_id}", flush=True)
            break
        except Exception:
            print(f"[sync] create_bucket failed, retrying in 30s:", flush=True)
            traceback.print_exc()
            time.sleep(30)

    print(f"Sync thread active for: hf://buckets/{bucket_id}", flush=True)
    while True:
        time.sleep(60)
        try:
            for table in ["bronze_table", "silver_table"]:
                local_path = f"/tmp/{table}"
                if os.path.exists(local_path):
                    sync_bucket(
                        local_path,
                        f"hf://buckets/{bucket_id}/{table}",
                        token=token,
                        ignore_patterns=["*.tmp", "*.crc", ".*"]
                    )
                else:
                    print(f"[sync] {local_path} does not exist yet, skipping", flush=True)
            print("Sync cycle successful.", flush=True)
        except Exception:
            print("Sync error:", flush=True)
            traceback.print_exc()

threading.Thread(target=sync_to_huggingface, daemon=True).start()

# --- 2. THE PYSPARK ENGINE ---
spark = SparkSession.builder \
    .appName("HF_Local_Sync_Pipeline") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,io.delta:delta-spark_2.12:3.1.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Add JKS/PKCS12 files to executor context
spark.sparkContext.addFile("truststore.jks")
spark.sparkContext.addFile("keystore.p12")

# --- 3. FOREACHBATCH WRITER ---
def write_batches(df, epoch_id):
    df.write.format("delta").mode("append").save("/tmp/bronze_table")
    df.filter(col("value") > 50.0).write.format("delta").mode("append").save("/tmp/silver_table")

# Kafka source with bounded offsets AND SSL passwords
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", os.getenv("KAFKA_URI")) \
    .option("subscribe", "test-topic") \
    .option("startingOffsets", "earliest") \
    .option("maxOffsetsPerTrigger", 10000) \
    .option("kafka.security.protocol", "SSL") \
    .option("kafka.ssl.truststore.location", SparkFiles.get("truststore.jks")) \
    .option("kafka.ssl.truststore.password", "changeit") \
    .option("kafka.ssl.keystore.location", SparkFiles.get("keystore.p12")) \
    .option("kafka.ssl.keystore.password", "changeit") \
    .load()

schema = StructType([
    StructField("id", IntegerType()),
    StructField("sensor_name", StringType()),
    StructField("value", DoubleType())
])
parsed_df = kafka_df.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json("json_str", schema).alias("data")).select("data.*")

query = parsed_df.writeStream \
    .foreachBatch(write_batches) \
    .option("checkpointLocation", "/tmp/checkpoints/main") \
    .trigger(processingTime="30 seconds") \
    .start()

# Handle Failures
try:
    query.awaitTermination()
except Exception:
    print(f"Query terminated with exception: {query.exception()}")
    sys.exit(1)  # Force GitHub Actions to fail if streaming fails
