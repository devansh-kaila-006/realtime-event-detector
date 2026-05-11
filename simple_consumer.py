"""
Simple Kafka to MongoDB consumer
Processes events from Kafka and stores them directly in MongoDB
"""

import json
from kafka import KafkaConsumer
from pymongo import MongoClient
from datetime import datetime

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["event_detector"]
collection = db["processed_events"]

# Connect to Kafka and consume from multiple topics
consumer = KafkaConsumer(
    'wiki_stream',
    'news_stream',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='simple_consumer_group'
)

print("Simple Consumer Started")
print("Listening for events from: wiki_stream, news_stream")
print("Press Ctrl+C to stop\n")

event_count = 0

try:
    for message in consumer:
        try:
            # Parse the event data
            data = json.loads(message.value)
            topic = message.topic

            # Create a unified event document
            event = {
                'title': data.get('title', 'Unknown Event'),
                'source_type': topic.replace('_stream', ''),  # wiki, news, etc.
                'timestamp': datetime.utcnow(),
                'ingested_at': datetime.utcnow(),
                'raw_data': data,
                'confidence_score': 0.7,  # Default confidence
                'event_cluster': 'general',
                'source': topic
            }

            # Insert into MongoDB with error handling
            try:
                result = collection.insert_one(event)
                if result.inserted_id:
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