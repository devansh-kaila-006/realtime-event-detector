"""
Simple Kafka to MongoDB Processor with NLP
Lightweight alternative to Spark with keyword extraction
"""

from pymongo import MongoClient
from kafka import KafkaConsumer
import json
import sys
from datetime import datetime
import re
from collections import Counter

# MongoDB connection
client = MongoClient("mongodb://localhost:27017/")
db = client["event_detector"]
processed_collection = db["processed_events"]
events_collection = db["events"]
keywords_collection = db["keywords"]

# Stopwords for keyword filtering
STOPWORDS = {
    "with", "that", "this", "from", "have", "were", "their", "about", "there",
    "would", "could", "should", "added", "using", "after", "before", "into",
    "while", "where", "which", "because", "reason", "guess", "slow", "they",
    "them", "then", "than", "been", "also", "said", "just", "some", "more",
    "such", "very", "only", "much", "many", "edit", "page", "article", "updated",
    "the", "a", "an", "and", "or", "but", "in", "on", "at", "to", "for",
    "of", "with", "by", "from", "as", "is", "was", "are", "were", "be", "been"
}

def extract_keywords(text):
    """Extract keywords from text"""
    if not text or not isinstance(text, str):
        return []

    # Clean text - remove special chars, convert to lowercase
    words = re.findall(r'\b[a-zA-Z]{4,}\b', text.lower())

    # Filter stopwords and count frequency
    words = [w for w in words if w not in STOPWORDS and len(w) > 3]

    # Get top 5 keywords
    word_counts = Counter(words)
    keywords = [word for word, count in word_counts.most_common(5)]

    return keywords

def process_text(data):
    """Process and extract text from event data"""
    text_parts = []

    # Add title
    if data.get("title"):
        text_parts.append(data["title"])

    # Add description or comment
    if data.get("description"):
        text_parts.append(data["description"])
    elif data.get("comment"):
        text_parts.append(data["comment"])

    # Add content if available
    if data.get("content"):
        text_parts.append(data["content"])

    return " ".join(text_parts)

# Kafka consumer
consumer = KafkaConsumer(
    "wiki_stream",
    "news_stream",
    "gdacs_stream",
    "financial_stream",
    bootstrap_servers=["localhost:9092"],
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

print("=" * 60)
print("[PROCESSOR] Enhanced Kafka to MongoDB Processor Started")
print("=" * 60)
print()
print("[INFO] Connected to Kafka: localhost:9092")
print("[INFO] Connected to MongoDB: localhost:27017")
print("[INFO] NLP Features: Keyword extraction enabled")
print("[INFO] Listening for messages...")
print()

message_count = 0
keyword_count = 0

try:
    for message in consumer:
        try:
            data = message.value
            topic = message.topic

            # Basic processing
            if isinstance(data, dict):
                # Fix source type mapping
                topic_name = topic.replace("_stream", "")
                if topic_name == "wiki":
                    source_type = "wikipedia"
                else:
                    source_type = topic_name

                data["source_type"] = source_type
                data["ingested_at"] = datetime.now()

                # Extract and process text
                text = process_text(data)
                data["clean_text"] = text.lower() if text else ""

                # Calculate word count
                data["word_count"] = len(text.split()) if text else 0

                # Extract keywords
                keywords = extract_keywords(text)
                data["keywords"] = keywords

                # Add basic NLP fields
                data["sentiment"] = "neutral"  # Placeholder for sentiment
                data["event_cluster"] = "general"  # Placeholder for classification
                data["entities"] = "{}"  # Placeholder for entities
                data["confidence_score"] = 0.5  # Placeholder confidence

                # Insert into processed_events
                result = processed_collection.insert_one(data)

                # Insert into simplified events
                events_collection.insert_one({
                    "title": data.get("title", "N/A"),
                    "source_type": data.get("source_type"),
                    "timestamp": data.get("timestamp"),
                    "published_at": data.get("published_at"),
                    "word_count": data.get("word_count")
                })

                # Insert keywords
                for keyword in keywords:
                    keywords_collection.update_one(
                        {"keyword": keyword, "source_type": source_type},
                        {"$inc": {"count": 1}},
                        upsert=True
                    )
                    keyword_count += 1

                message_count += 1

                if message_count % 10 == 0:
                    title_preview = data.get('title', 'N/A')[:40] if data.get('title') else 'N/A'
                    print(f"[PROCESSED] {message_count} messages | {source_type}: {title_preview}... | Keywords: {len(keywords)}")

        except Exception as e:
            print(f"[ERROR] Failed to process message: {e}")
            continue

except KeyboardInterrupt:
    print()
    print("=" * 60)
    print(f"[STOPPED] Processed {message_count} total messages")
    print(f"[STOPPED] Extracted {keyword_count} total keywords")
    print("=" * 60)

except Exception as e:
    print(f"[FATAL ERROR] {e}")
    sys.exit(1)
