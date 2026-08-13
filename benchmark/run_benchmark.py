import json
import time
from datetime import datetime
from kafka import KafkaProducer

KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC = "events_stream"
NUM_EVENTS = 10000

def run_benchmark():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )

    print(f"🚀 Starting Benchmark: Flooding Kafka with {NUM_EVENTS} events...")
    
    start_time = time.time()
    
    for i in range(NUM_EVENTS):
        # We deliberately create cross-stream correlation triggers
        # Every 500th event is a coordinated "Apple" burst across different sources
        
        is_burst = i % 500 == 0
        source = "financial" if i % 2 == 0 else "news"
        
        event = {
            "title": f"Benchmark Event {i}",
            "description": "Apple announces new breakthrough." if is_burst else f"Standard event description {i}.",
            "source_type": source,
            "timestamp": datetime.utcnow().isoformat(),
            "_raw_text": "Apple announces new breakthrough." if is_burst else f"Standard event {i} with nothing special."
        }
        
        producer.send(KAFKA_TOPIC, value=event)
        
        if i % 1000 == 0:
            print(f" -> Sent {i} events...")

    producer.flush()
    duration = time.time() - start_time
    print(f"✅ Finished sending {NUM_EVENTS} events in {duration:.2f} seconds.")
    print(f"⚡ Ingestion Throughput: {NUM_EVENTS/duration:.2f} events/sec")
    print("Now monitor the Spark console for processing throughput and latency.")

if __name__ == "__main__":
    run_benchmark()
