"""
Simple Kafka to MongoDB consumer
Processes events from Kafka and stores them directly in MongoDB
"""

import json
from kafka import KafkaConsumer
from pymongo import MongoClient
from datetime import datetime
import re
from collections import Counter

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["event_detector"]
processed_collection = db["processed_events"]
events_collection = db["events"]
keywords_collection = db["keywords"]

STOPWORDS = {
    "the", "a", "an", "and", "or", "but", "in", "on", "at", "to", "for",
    "of", "with", "by", "from", "as", "is", "was", "are", "were", "be", "been",
    "this", "that", "these", "those", "will", "would", "could", "should",
    "about", "after", "before", "into", "over", "under", "between", "during"
}


def extract_keywords(text: str) -> list[str]:
    if not text:
        return []

    words = re.findall(r"\b[a-zA-Z]{4,}\b", text.lower())
    filtered = [word for word in words if word not in STOPWORDS]

    return [word for word, _ in Counter(filtered).most_common(5)]


def resolve_event_cluster(source_type: str) -> str:
    cluster_map = {
        "wikipedia": "wiki",
        "wiki": "wiki",
        "news": "news",
        "gdacs": "disaster",
        "financial": "finance",
    }
    return cluster_map.get(source_type, "general")

# Connect to Kafka and consume from multiple topics
consumer = KafkaConsumer(
    'wiki_stream',
    'news_stream',
    'gdacs_stream',
    'financial_stream',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='simple_consumer_group'
)

print("Simple Consumer Started")
print("Listening for events from: wiki_stream, news_stream, gdacs_stream, financial_stream")
print("Press Ctrl+C to stop\n")

event_count = 0

try:
    for message in consumer:
        try:
            # Parse the event data
            data = json.loads(message.value)
            topic = message.topic

            source_type = topic.replace("_stream", "")
            if source_type == "wiki":
                source_type = "wikipedia"
            event_cluster = resolve_event_cluster(source_type)

            text = " ".join(
                part for part in [
                    data.get("title"),
                    data.get("description"),
                    data.get("comment"),
                    data.get("content")
                ] if isinstance(part, str) and part.strip()
            )
            keywords = extract_keywords(text)

            # Create a unified processed event document
            event = {
                'title': data.get('title', 'Unknown Event'),
                'source_type': source_type,
                'timestamp': datetime.utcnow(),
                'ingested_at': datetime.utcnow(),
                'raw_data': data,
                'url': data.get('url') or data.get('source_url') or data.get('link'),
                'source_url': data.get('source_url') or data.get('url') or data.get('link'),
                'confidence_score': 0.7,  # Default confidence
                'event_cluster': event_cluster,
                'source': topic,
                'clean_text': text.lower(),
                'word_count': len(text.split()) if text else 0,
                'keywords': keywords
            }

            # Insert into MongoDB with error handling
            try:
                result = processed_collection.insert_one(event)
                if result.inserted_id:
                    events_collection.insert_one({
                        "title": event.get("title"),
                        "source_type": source_type,
                        "timestamp": event.get("timestamp"),
                        "word_count": event.get("word_count")
                    })

                    for keyword in keywords:
                        keywords_collection.update_one(
                            {"keyword": keyword, "source_type": source_type},
                            {"$inc": {"count": 1}},
                            upsert=True
                        )

                    event_count += 1

                    # Print status every 10 events
                    if event_count % 10 == 0:
                        print(f"Processed {event_count} events | Latest: {data.get('title', 'Unknown')[:50]}")
                else:
                    print(f"Failed to insert event: {data.get('title', 'Unknown')[:50]}")

            except Exception as db_error:
                print(f"Database error: {db_error}")
                continue

        except Exception as e:
            print(f"Error processing event: {e}")
            continue

except KeyboardInterrupt:
    print(f"\nConsumer stopped. Total events processed: {event_count}")

except Exception as e:
    print(f"Fatal error: {e}")
