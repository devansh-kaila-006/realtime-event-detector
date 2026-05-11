"""
Spark Structured Streaming Consumer
Kafka → Spark → MongoDB
Stable Windows-Compatible Version
"""

import os
import sys
import json

# ============================================================
# WINDOWS FIXES
# ============================================================

os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
os.environ["HADOOP_HOME"] = os.path.dirname(os.path.dirname(__file__))
os.environ["spark.hadoop.io.native.lib.available"] = "false"

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

# ============================================================
# PYSPARK IMPORTS
# ============================================================

from pyspark.sql import SparkSession, DataFrame

from pyspark.sql.functions import (
    col,
    from_json,
    concat_ws,
    coalesce,
    lit,
    current_timestamp,
    lower,
    regexp_replace,
    split,
    size,
    udf
)

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType
)

# ============================================================
# MONGODB
# ============================================================

from pymongo import MongoClient

# ============================================================
# ADVANCED NLP IMPORTS
# ============================================================

from spark.nlp_pipeline import (
    extract_entities,
    classify_event,
    extract_keywords_single,
    preprocess_text
)

try:
    from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
    VADER_AVAILABLE = True
except ImportError:
    VADER_AVAILABLE = False
    print("[NLP] VADER not available - sentiment analysis disabled. Install with: pip install vaderSentiment")

# ============================================================
# CONFIG
# ============================================================

KAFKA_BOOTSTRAP = "localhost:9092"

WIKI_TOPIC = "wiki_stream"
NEWS_TOPIC = "news_stream"
GDACS_TOPIC = "gdacs_stream"
FINANCIAL_TOPIC = "financial_stream"

# ============================================================
# SPARK SESSION
# ============================================================

spark = SparkSession.builder \
    .appName("RealtimeEventDetector") \
    .master("local[2]") \
    .config("spark.driver.host", "127.0.0.1") \
    .config("spark.sql.shuffle.partitions", "2") \
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
        "org.mongodb.spark:mongo-spark-connector_2.12:10.2.0"
    ) \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

print("\n" + "=" * 60)
print("SPARK STREAMING + NLP STARTED")
print("=" * 60 + "\n")

# ============================================================
# MONGODB CONNECTION
# ============================================================

mongo_client = MongoClient("mongodb://localhost:27017/")

mongo_db = mongo_client["event_detector"]

processed_collection = mongo_db["processed_events"]

events_collection = mongo_db["events"]

keywords_collection = mongo_db["keywords"]

# ============================================================
# SCHEMAS
# ============================================================

WIKI_SCHEMA = StructType([
    StructField("title", StringType(), True),
    StructField("user", StringType(), True),
    StructField("wiki", StringType(), True),
    StructField("server_name", StringType(), True),
    StructField("comment", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("length_old", LongType(), True),
    StructField("length_new", LongType(), True),
    StructField("type", StringType(), True),
    StructField("namespace", LongType(), True),
])

NEWS_SCHEMA = StructType([
    StructField("title", StringType(), True),
    StructField("description", StringType(), True),
    StructField("content", StringType(), True),
    StructField("source", StringType(), True),
    StructField("author", StringType(), True),
    StructField("published_at", StringType(), True),
    StructField("url", StringType(), True),
    StructField("category", StringType(), True),
])

GDACS_SCHEMA = StructType([
    StructField("title", StringType(), True),
    StructField("description", StringType(), True),
    StructField("disaster_type", StringType(), True),
    StructField("severity", StringType(), True),
    StructField("latitude", StringType(), True),
    StructField("longitude", StringType(), True),
    StructField("alert_level", StringType(), True),
    StructField("source_url", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("source_type", StringType(), True),
])

FINANCIAL_SCHEMA = StructType([
    StructField("title", StringType(), True),
    StructField("symbol", StringType(), True),
    StructField("index_name", StringType(), True),
    StructField("current_price", StringType(), True),
    StructField("price_change_percent", StringType(), True),
    StructField("anomaly_type", StringType(), True),
    StructField("severity", StringType(), True),
    StructField("description", StringType(), True),
    StructField("high", StringType(), True),
    StructField("low", StringType(), True),
    StructField("volume_spike", StringType(), True),
    StructField("market", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("source_type", StringType(), True),
])

# ============================================================
# READ KAFKA STREAM
# ============================================================

def read_kafka(topic: str, schema: StructType):

    raw_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("subscribe", topic) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()

    parsed_df = raw_df.selectExpr(
        "CAST(value AS STRING)"
    ).select(
        from_json(col("value"), schema).alias("data")
    ).select("data.*")

    return parsed_df

# ============================================================
# ADVANCED NLP PROCESSING
# ============================================================

def enrich(df: DataFrame, text_col: str):

    # Basic text cleaning
    enriched_df = df \
        .withColumn(
            "clean_text",
            lower(
                regexp_replace(
                    col(text_col),
                    "[^a-zA-Z0-9 ]",
                    ""
                )
            )
        ) \
        .withColumn(
            "clean_text",
            regexp_replace(
                col("clean_text"),
                "\\s+",
                " "
            )
        ) \
        .withColumn(
            "clean_text",
            regexp_replace(
                col("clean_text"),
                "^\\s+|\\s+$",
                ""
            )
        ) \
        .withColumn(
            "words",
            split(
                col("clean_text"),
                "\\s+"
            )
        ) \
        .withColumn(
            "word_count",
            size(col("words"))
        ) \
        .withColumn(
            "ingested_at",
            current_timestamp()
        )

    # Add advanced NLP features (as strings to avoid Spark type issues)
    try:
        # Named entities extraction
        enriched_df = enriched_df.withColumn(
            "entities",
            extract_entities(col("clean_text"))
        )

        # Event classification
        enriched_df = enriched_df.withColumn(
            "event_cluster",
            classify_event(col("clean_text"))
        )

        # Keywords extraction
        enriched_df = enriched_df.withColumn(
            "keywords",
            extract_keywords_single(col("clean_text"))
        )

        # Basic confidence scoring (simplified)
        enriched_df = enriched_df.withColumn(
            "confidence_score",
            lit(0.5)  # Placeholder - will be computed in batch processing
        )

        # Sentiment analysis
        if VADER_AVAILABLE:
            sentiment_udf = udf(analyze_sentiment, StringType())
            enriched_df = enriched_df.withColumn(
                "sentiment",
                sentiment_udf(col("clean_text"))
            )
        else:
            enriched_df = enriched_df.withColumn(
                "sentiment",
                lit("neutral")
            )

    except Exception as e:
        print(f"[NLP] Error in advanced NLP enrichment: {e}")
        # Add basic columns if advanced processing fails
        enriched_df = enriched_df \
            .withColumn("entities", lit("{}")) \
            .withColumn("event_cluster", lit("general")) \
            .withColumn("keywords", lit("[]")) \
            .withColumn("confidence_score", lit(0.0)) \
            .withColumn("sentiment", lit("neutral"))

    return enriched_df

# ============================================================
# SENTIMENT ANALYSIS
# ============================================================

def analyze_sentiment(text: str) -> str:
    """Analyze sentiment using VADER."""
    if not text or not VADER_AVAILABLE:
        return "neutral"

    try:
        analyzer = SentimentIntensityAnalyzer()
        scores = analyzer.polarity_scores(str(text))

        if scores['compound'] >= 0.05:
            return "positive"
        elif scores['compound'] <= -0.05:
            return "negative"
        else:
            return "neutral"
    except Exception:
        return "neutral"

# ============================================================
# INSERT KEYWORDS
# ============================================================

def insert_keywords(doc):

    for word in doc.get("words", []):

        if len(word) > 3:

            keywords_collection.insert_one({
                "keyword": word,
                "source_type": doc.get("source_type")
            })

# ============================================================
# PROCESS WIKI BATCH
# ============================================================

def process_wiki_batch(batch_df, batch_id):

    if batch_df.isEmpty():
        return

    rows = batch_df.collect()

    print(f"\n[WIKI BATCH {batch_id}] {len(rows)} events")

    for row in rows:

        doc = json.loads(
            json.dumps(
                row.asDict(),
                default=str
            )
        )

        doc["source_type"] = "wikipedia"

        # FULL PROCESSED EVENT
        result = processed_collection.insert_one(doc)

        # SIMPLIFIED EVENT
        events_collection.insert_one({
            "title": doc.get("title"),
            "source_type": doc.get("source_type"),
            "timestamp": doc.get("timestamp"),
            "word_count": doc.get("word_count")
        })

        # KEYWORDS
        insert_keywords(doc)

        print("Inserted into MongoDB")

        doc["_id"] = str(result.inserted_id)

        print("=" * 60)
        print("LIVE WIKIPEDIA EVENT")
        print(json.dumps(doc, indent=2, default=str))
        print("=" * 60)

# ============================================================
# PROCESS NEWS BATCH
# ============================================================

def process_news_batch(batch_df, batch_id):

    if batch_df.isEmpty():
        return

    rows = batch_df.collect()

    print(f"\n[NEWS BATCH {batch_id}] {len(rows)} articles")

    for row in rows:

        doc = json.loads(
            json.dumps(
                row.asDict(),
                default=str
            )
        )

        doc["source_type"] = "news"

        # FULL PROCESSED EVENT
        result = processed_collection.insert_one(doc)

        # SIMPLIFIED EVENT
        events_collection.insert_one({
            "title": doc.get("title"),
            "source_type": doc.get("source_type"),
            "published_at": doc.get("published_at"),
            "word_count": doc.get("word_count")
        })

        # KEYWORDS
        insert_keywords(doc)

        print("Inserted into MongoDB")

        doc["_id"] = str(result.inserted_id)

        print("=" * 60)
        print("LIVE NEWS EVENT")
        print(json.dumps(doc, indent=2, default=str))
        print("=" * 60)

# ============================================================
# PROCESS GDACS BATCH
# ============================================================

def process_gdacs_batch(batch_df, batch_id):

    if batch_df.isEmpty():
        return

    rows = batch_df.collect()

    print(f"\n[GDACS BATCH {batch_id}] {len(rows)} disaster alerts")

    for row in rows:

        doc = json.loads(
            json.dumps(
                row.asDict(),
                default=str
            )
        )

        # GDACS already has source_type set
        if "source_type" not in doc:
            doc["source_type"] = "gdacs"

        # FULL PROCESSED EVENT
        result = processed_collection.insert_one(doc)

        # SIMPLIFIED EVENT
        events_collection.insert_one({
            "title": doc.get("title"),
            "source_type": doc.get("source_type"),
            "timestamp": doc.get("timestamp"),
            "disaster_type": doc.get("disaster_type"),
            "severity": doc.get("severity")
        })

        # KEYWORDS
        insert_keywords(doc)

        print("Inserted into MongoDB")

        doc["_id"] = str(result.inserted_id)

        print("=" * 60)
        print("LIVE GDACS ALERT")
        print(json.dumps(doc, indent=2, default=str))
        print("=" * 60)

# ============================================================
# PROCESS FINANCIAL BATCH
# ============================================================

def process_financial_batch(batch_df, batch_id):

    if batch_df.isEmpty():
        return

    rows = batch_df.collect()

    print(f"\n[FINANCIAL BATCH {batch_id}] {len(rows)} market events")

    for row in rows:

        doc = json.loads(
            json.dumps(
                row.asDict(),
                default=str
            )
        )

        # Financial already has source_type set
        if "source_type" not in doc:
            doc["source_type"] = "financial"

        # FULL PROCESSED EVENT
        result = processed_collection.insert_one(doc)

        # SIMPLIFIED EVENT
        events_collection.insert_one({
            "title": doc.get("title"),
            "source_type": doc.get("source_type"),
            "timestamp": doc.get("timestamp"),
            "anomaly_type": doc.get("anomaly_type"),
            "severity": doc.get("severity")
        })

        # KEYWORDS
        insert_keywords(doc)

        print("Inserted into MongoDB")

        doc["_id"] = str(result.inserted_id)

        print("=" * 60)
        print("LIVE FINANCIAL EVENT")
        print(json.dumps(doc, indent=2, default=str))
        print("=" * 60)

# ============================================================
# BUILD STREAMS
# ============================================================

wiki_raw = read_kafka(
    WIKI_TOPIC,
    WIKI_SCHEMA
)

news_raw = read_kafka(
    NEWS_TOPIC,
    NEWS_SCHEMA
)

gdacs_raw = read_kafka(
    GDACS_TOPIC,
    GDACS_SCHEMA
)

financial_raw = read_kafka(
    FINANCIAL_TOPIC,
    FINANCIAL_SCHEMA
)

# ============================================================
# WIKI ENRICHMENT
# ============================================================

wiki_enriched = wiki_raw \
    .withColumn(
        "_text",
        concat_ws(
            " ",
            col("title"),
            col("comment")
        )
    )

wiki_enriched = enrich(
    wiki_enriched,
    "_text"
).drop("_text")

# ============================================================
# NEWS ENRICHMENT
# ============================================================

news_enriched = news_raw \
    .withColumn(
        "_text",
        concat_ws(
            " ",
            coalesce(col("title"), lit("")),
            coalesce(col("description"), lit("")),
            coalesce(col("content"), lit(""))
        )
    )

news_enriched = enrich(
    news_enriched,
    "_text"
).drop("_text")

# ============================================================
# GDACS ENRICHMENT
# ============================================================

gdacs_enriched = gdacs_raw \
    .withColumn(
        "_text",
        concat_ws(
            " ",
            coalesce(col("title"), lit("")),
            coalesce(col("description"), lit(""))
        )
    )

gdacs_enriched = enrich(
    gdacs_enriched,
    "_text"
).drop("_text")

# ============================================================
# FINANCIAL ENRICHMENT
# ============================================================

financial_enriched = financial_raw \
    .withColumn(
        "_text",
        concat_ws(
            " ",
            coalesce(col("title"), lit("")),
            coalesce(col("description"), lit(""))
        )
    )

financial_enriched = enrich(
    financial_enriched,
    "_text"
).drop("_text")

# ============================================================
# START STREAMS
# ============================================================

wiki_query = wiki_enriched.writeStream \
    .foreachBatch(process_wiki_batch) \
    .outputMode("append") \
    .trigger(processingTime="10 seconds") \
    .option("checkpointLocation", "checkpoints/wiki") \
    .start()

news_query = news_enriched.writeStream \
    .foreachBatch(process_news_batch) \
    .outputMode("append") \
    .trigger(processingTime="10 seconds") \
    .option("checkpointLocation", "checkpoints/news") \
    .start()

gdacs_query = gdacs_enriched.writeStream \
    .foreachBatch(process_gdacs_batch) \
    .outputMode("append") \
    .trigger(processingTime="10 seconds") \
    .option("checkpointLocation", "checkpoints/gdacs") \
    .start()

financial_query = financial_enriched.writeStream \
    .foreachBatch(process_financial_batch) \
    .outputMode("append") \
    .trigger(processingTime="10 seconds") \
    .option("checkpointLocation", "checkpoints/financial") \
    .start()

# ============================================================
# WAIT FOR STREAMS
# ============================================================

spark.streams.awaitAnyTermination()