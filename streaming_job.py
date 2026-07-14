import os
import io
from huggingface_hub import HfApi
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# --- 1. THE GENIUS TURNAROUND: PRE-SEED THE FOLDERS ---
print("Pre-seeding Delta folders to bypass S3 marker crash...")
api = HfApi(token=os.getenv("HF_TOKEN")) # Ensure your token is available!
repo_id = f"{os.getenv('HF_NAMESPACE')}/{os.getenv('HF_BUCKET_NAME')}"

# Upload hidden .keep files to force HF to create the _delta_log directories
for table in ["bronze_table", "silver_table"]:
    try:
        api.upload_file(
            path_or_fileobj=io.BytesIO(b"dummy"),
            path_in_repo=f"{table}/_delta_log/.keep",
            repo_id=repo_id,
            repo_type="dataset"
        )
    except Exception as e:
        print(f"Directory marker {table} already exists or failed: {e}")

# --- 2. THE PYSPARK ENGINE ---
# Initialize PySpark with the MD5 fix and Single Driver Log Store to reduce API calls
spark = SparkSession.builder \
    .appName("HF_Direct_Delta_Stream") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,io.delta:delta-spark_2.12:3.1.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore") \
    .config("spark.driver.extraJavaOptions", "-Dcom.amazonaws.services.s3.disablePutObjectMD5Validation=true -Dcom.amazonaws.services.s3.disableGetObjectMD5Validation=true") \
    .config("spark.executor.extraJavaOptions", "-Dcom.amazonaws.services.s3.disablePutObjectMD5Validation=true -Dcom.amazonaws.services.s3.disableGetObjectMD5Validation=true") \
    .getOrCreate()

# Configure Hugging Face Bucket details
hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.endpoint", f"https://s3.hf.co/{os.getenv('HF_NAMESPACE')}")
hadoop_conf.set("fs.s3a.access.key", os.getenv("HF_S3_ACCESS_KEY"))
hadoop_conf.set("fs.s3a.secret.key", os.getenv("HF_S3_SECRET_KEY"))
hadoop_conf.set("fs.s3a.path.style.access", "true")
hadoop_conf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

bronze_path = f"s3a://{os.getenv('HF_BUCKET_NAME')}/bronze_table"
silver_path = f"s3a://{os.getenv('HF_BUCKET_NAME')}/silver_table"

# Connect to Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", os.getenv("KAFKA_URI")) \
    .option("subscribe", "test-topic") \
    .option("startingOffsets", "earliest") \
    .option("kafka.security.protocol", "SSL") \
    .option("kafka.ssl.truststore.location", "truststore.jks") \
    .option("kafka.ssl.truststore.password", "changeit") \
    .option("kafka.ssl.keystore.type", "PKCS12") \
    .option("kafka.ssl.keystore.location", "keystore.p12") \
    .option("kafka.ssl.keystore.password", "changeit") \
    .load()

# Parse data
schema = StructType([
    StructField("id", IntegerType()),
    StructField("sensor_name", StringType()),
    StructField("value", DoubleType())
])
parsed_df = kafka_df.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json("json_str", schema).alias("data")).select("data.*")

# --- 3. THE TRIGGER FIX (Saves your API Limits) ---
# Write EVERYTHING to Bronze
bronze_query = parsed_df.writeStream \
    .format("delta") \
    .option("checkpointLocation", "/tmp/checkpoints/bronze") \
    .trigger(processingTime="30 seconds") \
    .start(bronze_path)

# Branch the stream IN MEMORY for Silver (Filter for values > 50.0)
silver_query = parsed_df.filter(col("value") > 50.0).writeStream \
    .format("delta") \
    .option("checkpointLocation", "/tmp/checkpoints/silver") \
    .trigger(processingTime="30 seconds") \
    .start(silver_path)

print("Pipeline started! Writing directly to Hugging Face Delta Tables every 30 seconds...")
spark.streams.awaitAnyTermination(timeout=1800)
