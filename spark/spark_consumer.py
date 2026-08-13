import os
import sys

# Windows Spark fixes for local execution without Hadoop
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
os.environ["HADOOP_HOME"] = os.path.dirname(os.path.dirname(__file__))
os.environ["spark.hadoop.io.native.lib.available"] = "false"

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType
from pymongo import MongoClient

# Import our vectorized UDFs
from nlp_pipeline import vectorize_sentiment, vectorize_classification, vectorize_entities, vectorize_keywords

KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC = "events_stream"
MONGO_URI = "mongodb://localhost:27017/"

# Define the schema of incoming Kafka messages
schema = StructType([
    StructField("title", StringType(), True),
    StructField("description", StringType(), True),
    StructField("source_type", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("_raw_text", StringType(), True)
])

def create_spark_session():
    return SparkSession.builder \
        .appName("BigDataEventDetector") \
        .master("local[2]") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .getOrCreate()

def process_batch(df, epoch_id):
    if df.isEmpty():
        return
        
    print(f"\n[Spark Batch {epoch_id}] Processing {df.count()} events...")
    
    # We use PyMongo here because it's easier to configure locally than the Spark-Mongo connector jar
    # We collect the batch and insert using insert_many for high throughput
    records = [row.asDict() for row in df.collect()]
    
    if records:
        try:
            client = MongoClient(MONGO_URI)
            db = client["event_detector"]
            collection = db["processed_events"]
            collection.insert_many(records)
            print(f" -> Inserted {len(records)} events to MongoDB")
            client.close()
        except Exception as e:
            print(f" -> Error inserting to MongoDB: {e}")

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    print("\n==============================================")
    print("🚀 SPARK STRUCTURED STREAMING STARTED")
    print("==============================================\n")

    # Read from Kafka
    raw_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()

    # Parse JSON
    parsed_df = raw_df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

    # Apply PyArrow Pandas UDFs (Vectorized execution!)
    enriched_df = parsed_df \
        .withColumn("sentiment", vectorize_sentiment(col("_raw_text"))) \
        .withColumn("event_cluster", vectorize_classification(col("_raw_text"))) \
        .withColumn("entities", vectorize_entities(col("_raw_text"))) \
        .withColumn("keywords", vectorize_keywords(col("_raw_text"))) \
        .withColumn("ingested_at", current_timestamp().cast("string")) \
        .drop("_raw_text")

    # Write output to MongoDB via foreachBatch
    query = enriched_df.writeStream \
        .foreachBatch(process_batch) \
        .outputMode("append") \
        .trigger(processingTime="10 seconds") \
        .option("checkpointLocation", "checkpoints/events") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()
