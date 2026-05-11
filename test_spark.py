"""
Quick Spark Consumer Test
Tests if Spark can connect and read from Kafka
"""

from pyspark.sql import SparkSession
import sys

# Windows fixes
import os
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
os.environ["HADOOP_HOME"] = os.path.dirname(os.path.abspath(__file__))
os.environ["spark.hadoop.io.native.lib.available"] = "false"

print("[TEST] Creating Spark Session...")
try:
    spark = SparkSession.builder \
        .appName("TestConnection") \
        .master("local[1]") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.sql.shuffle.partitions", "1") \
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
        ) \
        .getOrCreate()

    print("[OK] Spark session created")

    print("[TEST] Reading from Kafka topic: wiki_stream...")
    df = spark.read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "wiki_stream") \
        .option("startingOffsets", "earliest") \
        .load()

    print("[OK] Kafka connection successful")

    print("[TEST] Counting messages...")
    count = df.count()
    print(f"[OK] Found {count} messages in wiki_stream")

    if count > 0:
        print("[TEST] Reading first message...")
        first = df.first()
        print(f"[OK] First message key: {first.key}, value length: {len(first.value)}")

    print()
    print("=" * 60)
    print("[SUCCESS] Spark-Kafka connection is working!")
    print("=" * 60)

    spark.stop()

except Exception as e:
    print(f"[ERROR] {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
