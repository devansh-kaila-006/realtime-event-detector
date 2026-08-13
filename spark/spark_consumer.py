import os
import sys
import time
import json
from datetime import datetime

# Keep the Windows OpenMP fix for PyTorch just in case
os.environ["KMP_DUPLICATE_LIB_OK"] = "TRUE"

from pymongo import MongoClient
from kafka import KafkaConsumer

# Import our vectorized NLP models
from nlp_pipeline import vectorize_sentiment, vectorize_embeddings, vectorize_entities, vectorize_keywords

KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC = "events_stream"
MONGO_URI = "mongodb://localhost:27017/"

# Basic in-memory state for burst detection
entity_frequency = {}

# Lightweight geocoding dictionary for NLP-based mapping
GEO_DICT = {
    "New York": (40.7128, -74.0060), "US": (37.0902, -95.7129), "USA": (37.0902, -95.7129),
    "London": (51.5074, -0.1278), "UK": (55.3781, -3.4360),
    "Tokyo": (35.6762, 139.6503), "Japan": (36.2048, 138.2529),
    "Paris": (48.8566, 2.3522), "France": (46.2276, 2.2137),
    "Sydney": (-33.8688, 151.2093), "Australia": (-25.2744, 133.7751),
    "Dubai": (25.2048, 55.2708), "UAE": (23.4241, 53.8478),
    "Singapore": (1.3521, 103.8198),
    "Mumbai": (19.0760, 72.8777), "India": (20.5937, 78.9629),
    "São Paulo": (-23.5505, -46.6333), "Brazil": (-14.2350, -51.9253),
    "Moscow": (55.7558, 37.6173), "Russia": (61.5240, 105.3188),
    "China": (35.8617, 104.1954), "Beijing": (39.9042, 116.4074),
    "Germany": (51.1657, 10.4515), "Berlin": (52.5200, 13.4050)
}

def process_batch(records, epoch_id):
    if not records:
        return
        
    start_time = time.time()
    batch_count = len(records)
    
    print(f"\n[Batch {epoch_id}] Processing {batch_count} events...")
    
    meta_events = []
    
    # 1. Apply Deep Learning NLP
    for r in records:
        r['sentiment'] = vectorize_sentiment(r.get('_raw_text', ''))
        r['embeddings'] = vectorize_embeddings(r.get('_raw_text', ''))
        r['entities'] = vectorize_entities(r.get('_raw_text', ''))
        r['keywords'] = vectorize_keywords(r.get('_raw_text', ''))
        r['ingested_at'] = datetime.utcnow().isoformat()
        
        # NLP Geocoding Fallback
        if 'lat' not in r or 'lng' not in r:
            try:
                ents = json.loads(r['entities'])
                places = ents.get('GPE', []) + ents.get('LOC', [])
                for place in places:
                    if place in GEO_DICT:
                        r['lat'], r['lng'] = GEO_DICT[place]
                        break
            except Exception:
                pass
        
        # Remove raw text so it doesn't bloat the DB
        if '_raw_text' in r:
            del r['_raw_text']
    
    # 2. Cross-Stream Correlation & Anomaly Detection Logic
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
                batch_entities[e].add(r.get('source_type', 'unknown'))
                
                # Track historical velocity
                entity_frequency[e] = entity_frequency.get(e, 0) + 1
                
        except Exception:
            pass

    # Identify Meta-Events (Anomalies & Correlations)
    for entity, sources in batch_entities.items():
        is_cross_stream = len(sources) > 1
        is_burst = entity_frequency.get(entity, 0) > 10
        
        if is_cross_stream or is_burst:
            meta_event = {
                "title": f"META-EVENT DETECTED: '{entity}'",
                "description": f"Entity '{entity}' exhibited anomalous behavior. Sources involved: {', '.join(sources)}. Burst score: {entity_frequency.get(entity)}",
                "source_type": "system_alert",
                "sentiment": "NEGATIVE" if "gdacs" in sources else "NEUTRAL", 
                "event_cluster": "META_ALERT",
                "ingested_at": datetime.utcnow().isoformat(),
                "is_anomaly": True
            }
            if entity in GEO_DICT:
                meta_event["lat"], meta_event["lng"] = GEO_DICT[entity]
                
            meta_events.append(meta_event)
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
            print(f" -> DETECTED {len(meta_events)} CROSS-STREAM META-EVENTS!")
            
        client.close()
    except Exception as e:
        print(f" -> Error inserting to MongoDB: {e}")
        
    # Benchmarking Stats
    duration = time.time() - start_time
    throughput = batch_count / duration if duration > 0 else 0
    print(f" -> [Bench] Batch processed in {duration:.2f}s | Throughput: {throughput:.2f} events/sec")


def main():
    print("\n[Initializing Deep Learning Models (This may take a minute)...]")
    from nlp_pipeline import get_sentiment_pipeline, get_embedder, get_spacy
    get_sentiment_pipeline()
    get_embedder()
    get_spacy()
    print("[Models Loaded Successfully!]\n")

    print("\n==============================================")
    print(">>> PURE PYTHON KAFKA CONSUMER (STABLE EDITION)")
    print("==============================================\n")

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=[KAFKA_BOOTSTRAP],
        auto_offset_reset='latest',
        enable_auto_commit=True,
        group_id='event_detector_group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    batch = []
    last_batch_time = time.time()
    epoch_id = 0

    print(f"Listening to Kafka topic: {KAFKA_TOPIC}")

    try:
        for message in consumer:
            batch.append(message.value)
            
            current_time = time.time()
            # Process batch if it reaches 10 items or 5 seconds have passed
            if len(batch) >= 10 or (current_time - last_batch_time) > 5.0:
                process_batch(batch, epoch_id)
                batch = []
                last_batch_time = current_time
                epoch_id += 1
                
    except KeyboardInterrupt:
        print("\nStopping consumer...")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()
