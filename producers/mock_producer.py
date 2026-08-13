import json
import time
import random
import threading
from datetime import datetime
from kafka import KafkaProducer

KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC = "events_stream"

def create_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda x: json.dumps(x).encode("utf-8"),
        retries=5
    )

def mock_news(producer):
    categories = ["technology", "business", "politics", "health", "science"]
    while True:
        cat = random.choice(categories)
        title = f"Breaking {cat.capitalize()} News {random.randint(100, 999)}"
        event = {
            "title": title,
            "description": f"Important news regarding {cat} occurred.",
            "source_type": "news",
            "category": cat,
            "timestamp": datetime.utcnow().isoformat(),
            "_raw_text": f"{title} Important news regarding {cat} occurred."
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
        title = f"{severity.capitalize()} {d_type} Alert"
        event = {
            "title": title,
            "description": f"A {severity} severity {d_type.lower()} has been detected.",
            "source_type": "gdacs",
            "disaster_type": d_type,
            "severity": severity,
            "timestamp": datetime.utcnow().isoformat(),
            "_raw_text": f"{title} A {severity} severity {d_type.lower()} has been detected."
        }
        producer.send(KAFKA_TOPIC, value=event)
        print(f"[GDACS] -> Kafka | {event['timestamp']} | {title}")
        time.sleep(random.uniform(10, 30))

def mock_financial(producer):
    symbols = ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA"]
    while True:
        sym = random.choice(symbols)
        change = random.uniform(-10, 10)
        title = f"{sym} Market Movement {change:.2f}%"
        event = {
            "title": title,
            "symbol": sym,
            "price_change": change,
            "source_type": "financial",
            "timestamp": datetime.utcnow().isoformat(),
            "_raw_text": f"{title} Market detected a {change:.2f}% movement for {sym}."
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
