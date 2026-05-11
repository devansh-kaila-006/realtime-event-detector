"""
Wikipedia real-time edit stream → Kafka producer.
Connects to Wikimedia SSE stream, filters to English Wikipedia human edits,
and publishes clean JSON messages to the wiki_stream Kafka topic.
"""

import json
import time
import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from config.settings import KAFKA_BOOTSTRAP, KAFKA_WIKI_TOPIC

STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"

BLOCKED_PREFIXES = (
    "Category:", "File:", "Template:", "User:", "User talk:",
    "Talk:", "Wikipedia:", "Wikipedia talk:", "Module:",
    "Draft:", "Portal:", "Special:", "Help:", "MediaWiki:"
)


def create_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda x: json.dumps(x).encode("utf-8"),
        retries=5,
        acks="all"
    )


def build_message(data: dict) -> dict:
    return {
        "title":       data.get("title"),
        "user":        data.get("user"),
        "wiki":        data.get("wiki"),
        "server_name": data.get("server_name"),
        "comment":     data.get("comment", ""),
        "timestamp":   data.get("meta", {}).get("dt"),
        "length_old":  data.get("length", {}).get("old"),
        "length_new":  data.get("length", {}).get("new"),
        "type":        data.get("type"),          # edit | new | log
        "namespace":   data.get("namespace"),
    }


def stream_wikipedia(producer: KafkaProducer):
    headers = {
        "User-Agent": "RealTimeEventDetector/2.0",
        "Accept": "text/event-stream"
    }
    print(f"\n[Wiki Producer] Connecting to Wikimedia stream...\n")

    response = requests.get(STREAM_URL, headers=headers, stream=True, timeout=30)
    print("[Wiki Producer] Connected. Streaming events...\n")

    for raw_line in response.iter_lines():
        if not raw_line:
            continue
        line = raw_line.decode("utf-8")
        if not line.startswith("data:"):
            continue

        try:
            data = json.loads(line[5:].strip())
        except json.JSONDecodeError:
            continue

        title = data.get("title", "")
        wiki  = data.get("wiki", "")

        if wiki != "enwiki":
            continue
        if data.get("bot") is True:
            continue
        if not title.strip():
            continue
        if title.startswith(BLOCKED_PREFIXES):
            continue

        message = build_message(data)

        future = producer.send(KAFKA_WIKI_TOPIC, value=message)
        try:
            future.get(timeout=5)
        except KafkaError as e:
            print(f"[Wiki Producer] Kafka send error: {e}")
            continue

        print(f"[Wiki] {message['timestamp']} | {title[:60]}")


def main():
    producer = create_producer()
    while True:
        try:
            stream_wikipedia(producer)
        except requests.exceptions.ConnectionError as e:
            print(f"[Wiki Producer] Connection error: {e}")
            print("[Wiki Producer] Retrying in 5 seconds...")
            time.sleep(5)
        except Exception as e:
            print(f"[Wiki Producer] Unexpected error: {e}")
            time.sleep(5)


if __name__ == "__main__":
    main()