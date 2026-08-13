import os
import sys
import time
import json
from datetime import datetime

# Windows Spark fixes for local execution
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
os.environ["HADOOP_HOME"] = os.path.dirname(os.path.dirname(__file__))
os.environ["PATH"] = os.path.join(os.environ["HADOOP_HOME"], "bin") + os.pathsep + os.environ.get("PATH", "")
os.environ["spark.hadoop.io.native.lib.available"] = "false"

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pymongo import MongoClient

# Import our vectorized UDFs
from nlp_pipeline import vectorize_sentiment, vectorize_embeddings, vectorize_entities, vectorize_keywords

KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC = "events_stream"
MONGO_URI = "mongodb://localhost:27017/"

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
        .master("local[4]") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .config("spark.memory.fraction", "0.8") \
        .getOrCreate()

# Basic in-memory state for burst detection (driver side)
entity_frequency = {}

def process_batch(df, epoch_id):
    if df.isEmpty():
        return
        
    start_time = time.time()
    batch_count = df.count()
    
    print(f"\n[Spark Batch {epoch_id}] Processing {batch_count} events...")
    
    records = [row.asDict() for row in df.collect()]
    meta_events = []
    
    if records:
        # Cross-Stream Correlation & Anomaly Detection Logic
        batch_entities = {}
        
        for r in records:
            # Parse extracted entities
            try:
                ents = json.loads(r.get('entities', '{}'))
                # flatten entities
                all_ents = []
                for k, v in ents.items():
                    all_ents.extend(v)
                
                # Track occurrences by source
                for e in set(all_ents):
                    if e not in batch_entities:
                        batch_entities[e] = set()
                    batch_entities[e].add(r['source_type'])
                    
                    # Track historical velocity
                    entity_frequency[e] = entity_frequency.get(e, 0) + 1
                    
            except Exception:
                pass

        # Identify Meta-Events (Anomalies & Correlations)
        for entity, sources in batch_entities.items():
            # Condition 1: Cross-Stream Correlation (Mentioned in multiple sources in the same 10s batch)
            # Condition 2: Burst Anomaly (Mentioned over 10 times total rapidly)
            is_cross_stream = len(sources) > 1
            is_burst = entity_frequency.get(entity, 0) > 10
            
            if is_cross_stream or is_burst:
                meta_events.append({
                    "title": f"META-EVENT DETECTED: '{entity}'",
                    "description": f"Entity '{entity}' exhibited anomalous behavior. Sources involved: {', '.join(sources)}. Burst score: {entity_frequency.get(entity)}",
                    "source_type": "system_alert",
                    "sentiment": "NEGATIVE" if "gdacs" in sources else "NEUTRAL", 
                    "event_cluster": "META_ALERT",
                    "ingested_at": datetime.utcnow().isoformat(),
                    "is_anomaly": True
                })
                # Reset frequency after alert
                entity_frequency[entity] = 0

        # Save to MongoDB
        try:
            client = MongoClient(MONGO_URI)
            db = client["event_detector"]
            
            # Save normal events
            db["processed_events"].insert_many(records)
            print(f" -> Inserted {len(records)} raw events")
            
            # Save meta events if any
            if meta_events:
                db["meta_events"].insert_many(meta_events)
                print(f" -> 🔥 DETECTED {len(meta_events)} CROSS-STREAM META-EVENTS!")
                
            client.close()
        except Exception as e:
            print(f" -> Error inserting to MongoDB: {e}")
            
    # Benchmarking Stats
    duration = time.time() - start_time
    throughput = batch_count / duration if duration > 0 else 0
    print(f" -> ⚡ Batch processed in {duration:.2f}s | Throughput: {throughput:.2f} events/sec")

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    print("\n==============================================")
    print(">>> SPARK STRUCTURED STREAMING (PAPER EDITION)")
    print("==============================================\n")

    raw_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()

    parsed_df = raw_df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

    # Apply Deep Learning UDFs
    enriched_df = parsed_df \
        .withColumn("sentiment", vectorize_sentiment(col("_raw_text"))) \
        .withColumn("embeddings", vectorize_embeddings(col("_raw_text"))) \
        .withColumn("entities", vectorize_entities(col("_raw_text"))) \
        .withColumn("keywords", vectorize_keywords(col("_raw_text"))) \
        .withColumn("ingested_at", current_timestamp().cast("string")) \
        .drop("_raw_text")

    query = enriched_df.writeStream \
        .foreachBatch(process_batch) \
        .outputMode("append") \
        .trigger(processingTime="10 seconds") \
        .option("checkpointLocation", "checkpoints/events") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()
