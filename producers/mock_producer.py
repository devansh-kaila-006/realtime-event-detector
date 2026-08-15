import json
import time
import random
import threading
from datetime import datetime, timezone
from kafka import KafkaProducer

KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC = "events_stream"

def create_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda x: json.dumps(x).encode("utf-8"),
        retries=5
    )

CITIES = [
    {"name": "New York", "lat": 40.7128, "lng": -74.0060},
    {"name": "London", "lat": 51.5074, "lng": -0.1278},
    {"name": "Tokyo", "lat": 35.6762, "lng": 139.6503},
    {"name": "Paris", "lat": 48.8566, "lng": 2.3522},
    {"name": "Sydney", "lat": -33.8688, "lng": 151.2093},
    {"name": "Dubai", "lat": 25.2048, "lng": 55.2708},
    {"name": "Singapore", "lat": 1.3521, "lng": 103.8198},
    {"name": "Mumbai", "lat": 19.0760, "lng": 72.8777},
    {"name": "São Paulo", "lat": -23.5505, "lng": -46.6333},
    {"name": "Moscow", "lat": 55.7558, "lng": 37.6173}
]

def mock_news(producer):
    categories = ["technology", "business", "politics", "health", "science"]
    while True:
        cat = random.choice(categories)
        loc = random.choice(CITIES)
        title = f"Breaking {cat.capitalize()} News {random.randint(100, 999)}"
        event = {
            "title": title,
            "description": f"Important news regarding {cat} occurred in {loc['name']}.",
            "source_type": "news",
            "category": cat,
            "location": loc['name'],
            "lat": loc['lat'],
            "lng": loc['lng'],
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "_raw_text": f"{title} Important news regarding {cat} occurred in {loc['name']}."
        }
        producer.send(KAFKA_TOPIC, value=event)
        print(f"[News] -> Kafka | {event['timestamp']} | {title}")
        time.sleep(random.uniform(5, 15))

def mock_gdacs(producer):
    types = ["Earthquake", "Flood", "Hurricane"]
    severities = ["low", "moderate", "high", "extreme"]
    while True:
        d_type = random.choice(types)
        severity = random.choice(severities)
        loc = random.choice(CITIES)
        title = f"{severity.capitalize()} {d_type} Alert near {loc['name']}"
        event = {
            "title": title,
            "description": f"A {severity} severity {d_type.lower()} has been detected near {loc['name']}.",
            "source_type": "gdacs",
            "disaster_type": d_type,
            "severity": severity,
            "location": loc['name'],
            "lat": loc['lat'],
            "lng": loc['lng'],
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "_raw_text": f"{title} A {severity} severity {d_type.lower()} has been detected near {loc['name']}."
        }
        producer.send(KAFKA_TOPIC, value=event)
        print(f"[GDACS] -> Kafka | {event['timestamp']} | {title}")
        time.sleep(random.uniform(10, 30))

def mock_financial(producer):
    symbols = ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA"]
    while True:
        sym = random.choice(symbols)
        change = random.uniform(-10, 10)
        loc = random.choice(CITIES)
        title = f"{sym} Market Movement {change:.2f}%"
        event = {
            "title": title,
            "symbol": sym,
            "price_change": change,
            "source_type": "financial",
            "location": loc['name'],
            "lat": loc['lat'],
            "lng": loc['lng'],
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "_raw_text": f"{title} Market detected a {change:.2f}% movement for {sym} affecting trading hubs like {loc['name']}."
        }
        producer.send(KAFKA_TOPIC, value=event)
        print(f"[Finance] -> Kafka | {event['timestamp']} | {title}")
        time.sleep(random.uniform(3, 10))

def main():
    producer = create_producer()
    threads = [
        threading.Thread(target=mock_news, args=(producer,), daemon=True),
        threading.Thread(target=mock_gdacs, args=(producer,), daemon=True),
        threading.Thread(target=mock_financial, args=(producer,), daemon=True)
    ]
    for t in threads:
        t.start()
    
    # Keep main thread alive
    while True:
        time.sleep(1)

if __name__ == "__main__":
    main()
