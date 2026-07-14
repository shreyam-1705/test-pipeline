import os
import time
import threading
from huggingface_hub import HfApi
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

def sync_to_huggingface():
    api = HfApi(token=os.getenv("HF_TOKEN"))
    repo_id = f"{os.getenv('HF_NAMESPACE')}/{os.getenv('HF_BUCKET_NAME')}"
    while True:
        time.sleep(60)
        try:
            for table in ["bronze_table", "silver_table"]:
                local_path = f"/tmp/{table}"
                if os.path.exists(local_path):
                    api.upload_folder(
                        folder_path=local_path,
                        repo_id=repo_id,
                        repo_type="dataset",
                        path_in_repo=table,
                        ignore_patterns=["*.tmp", "*.crc", ".*"]
                    )
        except Exception as e:
            print(f"Sync error: {e}")

threading.Thread(target=sync_to_huggingface, daemon=True).start()

spark = SparkSession.builder \
    .appName("HF_Local_Sync_Pipeline") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,io.delta:delta-spark_2.12:3.1.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

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

schema = StructType([StructField("id", IntegerType()), StructField("sensor_name", StringType()), StructField("value", DoubleType())])
parsed_df = kafka_df.selectExpr("CAST(value AS STRING) as json_str").select(from_json("json_str", schema).alias("data")).select("data.*")

parsed_df.writeStream.format("delta").option("checkpointLocation", "/tmp/checkpoints/bronze").start("/tmp/bronze_table")
parsed_df.filter(col("value") > 50.0).writeStream.format("delta").option("checkpointLocation", "/tmp/checkpoints/silver").start("/tmp/silver_table")

spark.streams.awaitAnyTermination()
