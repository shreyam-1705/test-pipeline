import os
import time
import threading
import sys
import inspect
import traceback
import signal
import huggingface_hub
from huggingface_hub import create_bucket, sync_bucket
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark import SparkFiles

print(f"[sync] huggingface_hub version: {huggingface_hub.__version__}", flush=True)

_SYNC_PARAMS = set(inspect.signature(sync_bucket).parameters.keys())
print(f"[sync] sync_bucket accepts: {sorted(_SYNC_PARAMS)}", flush=True)

_FILTER_PATTERNS = ["*.tmp", "*.crc", ".*"]


def _sync_kwargs():
    kwargs = {}
    if "exclude" in _SYNC_PARAMS:
        kwargs["exclude"] = _FILTER_PATTERNS
    elif "ignore_patterns" in _SYNC_PARAMS:
        kwargs["ignore_patterns"] = _FILTER_PATTERNS
    return kwargs


NAMESPACE = os.getenv("HF_NAMESPACE")
BUCKET_NAME = os.getenv("HF_BUCKET_NAME")
TOKEN = os.getenv("HF_TOKEN")
BUCKET_ID = f"{NAMESPACE}/{BUCKET_NAME}"

# How many local <-> remote paths we keep in sync, checkpoints included
SYNC_TARGETS = {
    "bronze_table": "/tmp/bronze_table",
    "silver_table": "/tmp/silver_table",
    "checkpoints": "/tmp/checkpoints",
}

RUN_DURATION_SECONDS = int(os.getenv("RUN_DURATION_SECONDS", "1800"))  # default 30 min


# --- Restore checkpoint (and any prior tables) from HF before starting ---
def restore_from_hf():
    for name, local_path in SYNC_TARGETS.items():
        remote_path = f"hf://buckets/{BUCKET_ID}/{name}"
        os.makedirs(local_path, exist_ok=True)
        try:
            sync_bucket(remote_path, local_path, token=TOKEN, **_sync_kwargs())
            print(f"[restore] Restored {name} from {remote_path}", flush=True)
        except Exception:
            print(f"[restore] Nothing to restore for {name} (likely first run):", flush=True)
            traceback.print_exc()


# --- Bucket setup ---
def ensure_bucket():
    while True:
        try:
            create_bucket(BUCKET_ID, token=TOKEN, exist_ok=True)
            print(f"[sync] Bucket ready: hf://buckets/{BUCKET_ID}", flush=True)
            return
        except Exception:
            print("[sync] create_bucket failed, retrying in 30s:", flush=True)
            traceback.print_exc()
            time.sleep(30)


# --- One shared function to push everything to HF (used by loop, on exit, on signal) ---
_sync_lock = threading.Lock()


def sync_all_to_hf(tag="periodic"):
    with _sync_lock:
        for name, local_path in SYNC_TARGETS.items():
            remote_path = f"hf://buckets/{BUCKET_ID}/{name}"
            if os.path.exists(local_path):
                try:
                    sync_bucket(local_path, remote_path, token=TOKEN, **_sync_kwargs())
                except Exception:
                    print(f"[sync:{tag}] Failed syncing {name}:", flush=True)
                    traceback.print_exc()
            else:
                print(f"[sync:{tag}] {local_path} does not exist yet, skipping", flush=True)
        print(f"[sync:{tag}] Sync cycle complete.", flush=True)


def sync_loop():
    while True:
        time.sleep(60)
        sync_all_to_hf(tag="periodic")


print(f"[sync] HF_NAMESPACE={NAMESPACE} HF_BUCKET_NAME={BUCKET_NAME} "
      f"HF_TOKEN={'set' if TOKEN else 'MISSING'} RUN_DURATION_SECONDS={RUN_DURATION_SECONDS}", flush=True)

ensure_bucket()
restore_from_hf()  # pull down last checkpoint + tables before Spark starts

threading.Thread(target=sync_loop, daemon=True).start()

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


# Log each micro-batch's Kafka offset range so you can see resume-vs-reprocess behavior
def log_progress():
    while query.isActive:
        p = query.lastProgress
        if p and "sources" in p and p["sources"]:
            src = p["sources"][0]
            print(f"[offsets] startOffset={src.get('startOffset')} "
                  f"endOffset={src.get('endOffset')} "
                  f"numInputRows={p.get('numInputRows')}", flush=True)
        time.sleep(15)


threading.Thread(target=log_progress, daemon=True).start()


# Graceful shutdown -> stop query, do one final sync (catches checkpoint's last commit), exit clean
def handle_shutdown(signum, frame):
    print(f"[shutdown] Signal {signum} received, stopping query...", flush=True)
    try:
        query.stop()
    except Exception:
        traceback.print_exc()
    sync_all_to_hf(tag="shutdown")
    sys.exit(0)


signal.signal(signal.SIGTERM, handle_shutdown)
signal.signal(signal.SIGINT, handle_shutdown)

# Handle Failures / bounded run
try:
    query.awaitTermination(timeout=RUN_DURATION_SECONDS)
    if query.isActive:
        print(f"[shutdown] RUN_DURATION_SECONDS={RUN_DURATION_SECONDS} reached, stopping query...", flush=True)
        query.stop()
    sync_all_to_hf(tag="final")
except Exception:
    print(f"Query terminated with exception: {query.exception()}", flush=True)
    sync_all_to_hf(tag="error")
    sys.exit(1)
