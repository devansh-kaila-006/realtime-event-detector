"""
Wikipedia real-time edit stream → Kafka producer (ENHANCED WITH SAMPLING)
Connects to Wikimedia SSE stream and publishes a SAMPLE of events to create balanced data.
"""

import json
import time
import random
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

# ============================================================
# SAMPLING CONFIGURATION - Adjust to control Wikipedia volume
# ============================================================

# Only send this percentage of Wikipedia events (10% = 1 in 10 events)
WIKIPEDIA_SAMPLING_RATE = 0.1  # 10% sampling - much more permissive

# Minimum edit size to consider (filter out small edits)
MIN_EDIT_SIZE = 50  # Only send edits with at least 50 character changes (was 100)

# Only process events from these namespaces (0 = main articles only)
ALLOWED_NAMESPACES = [0]

# Minimum edit length for title
MIN_TITLE_LENGTH = 15  # Filter out short titles

# Block these patterns in titles
BLOCKED_PATTERNS = [
    "Draft:", "Draft talk:", "User:", "User talk:", "Talk:",
    "Wikipedia:", "Wikipedia talk:", "File:", "Template:",
    "Category:", "Portal:", "Module:", "Help:", "MediaWiki:",
    "List of", "list of", "Lists of", "lists of"  # Filter out list pages
]

# Delay between processing events (seconds)
PROCESSING_DELAY = 1.0  # Add delay to reduce volume and improve quality control


def create_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda x: json.dumps(x, default=str).encode("utf-8"),
        retries=5,
        acks="all"
    )


def build_message(data: dict) -> dict:
    title = data.get("title", "")
    # Create proper Wikipedia URL
    wiki_url = f"https://en.wikipedia.org/wiki/{title.replace(' ', '_')}"

    return {
        "title":       title,
        "url":         wiki_url,  # Add clickable URL
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


def should_send_event(data: dict, message: dict) -> bool:
    """
    Determine if a Wikipedia event should be sent based on filtering rules.
    Returns True if event should be sent, False otherwise.
    """

    # Filter 1: Namespace filter (only main articles)
    namespace = data.get("namespace")
    if namespace not in ALLOWED_NAMESPACES:
        return False

    # Filter 2: Minimum edit size (ignore tiny edits)
    length_old = message.get('length_old', 0)
    length_new = message.get('length_new', 0)
    edit_size = abs(length_new - length_old)

    if edit_size < MIN_EDIT_SIZE:
        return False

    # Filter 3: Skip bot edits
    if data.get("bot") is True:
        return False

    # Filter 4: Skip blocked prefixes and patterns
    title = data.get("title", "")
    if title.startswith(BLOCKED_PREFIXES):
        return False

    # Filter 5: Check blocked patterns
    for pattern in BLOCKED_PATTERNS:
        if pattern.lower() in title.lower():
            return False

    # Filter 6: Minimum title length
    if len(title.strip()) < MIN_TITLE_LENGTH:
        return False

    # Filter 7: Skip very short titles or numeric-only titles
    if len(title.strip()) < 10 or title.strip().isdigit():
        return False

    # Filter 8: Skip titles with special characters that indicate low quality
    # Removed this filter - too restrictive

    # Filter 9: Sampling rate (only send X% of events)
    if random.random() > WIKIPEDIA_SAMPLING_RATE:
        return False

    return True


def stream_wikipedia(producer: KafkaProducer):
    headers = {
        "User-Agent": "RealTimeEventDetector/2.0",
        "Accept": "text/event-stream"
    }

    print(f"\n[Wiki Producer] Starting with SAMPLING enabled")
    print(f"[Wiki Producer] Sampling rate: {WIKIPEDIA_SAMPLING_RATE*100}%")
    print(f"[Wiki Producer] Minimum edit size: {MIN_EDIT_SIZE} characters")
    print(f"[Wiki Producer] Connecting to Wikimedia stream...\n")

    response = requests.get(STREAM_URL, headers=headers, stream=True, timeout=30)
    print("[Wiki Producer] Connected. Streaming events...\n")

    events_seen = 0
    events_sent = 0
    events_filtered = 0

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

        events_seen += 1

        title = data.get("title", "")
        wiki  = data.get("wiki", "")

        if wiki != "enwiki":
            continue

        message = build_message(data)

        # Apply all filtering rules
        if should_send_event(data, message):
            try:
                future = producer.send(KAFKA_WIKI_TOPIC, value=message)
                future.get(timeout=5)
                events_sent += 1

                # Print progress every 50 events
                if events_sent % 50 == 0:
                    sampling_pct = (events_sent / events_seen * 100) if events_seen > 0 else 0
                    print(f"[Wiki] Sent: {events_sent:,} | Seen: {events_seen:,} | Sampling: {sampling_pct:.1f}% | Latest: {title[:50]}")

            except KafkaError as e:
                print(f"[Wiki Producer] Kafka send error: {e}")
                continue
        else:
            events_filtered += 1

        # Add small delay to reduce processing speed
        time.sleep(PROCESSING_DELAY)


def main():
    print("=" * 60)
    print("[Wiki Producer] Enhanced Wikipedia Producer with Sampling")
    print("=" * 60)
    print(f"[CONFIG] Sampling Rate: {WIKIPEDIA_SAMPLING_RATE*100}%")
    print(f"[CONFIG] Min Edit Size: {MIN_EDIT_SIZE} characters")
    print(f"[CONFIG] Allowed Namespaces: {ALLOWED_NAMESPACES}")
    print(f"[CONFIG] Processing Delay: {PROCESSING_DELAY}s")
    print("=" * 60)
    print()

    producer = create_producer()

    try:
        while True:
            try:
                stream_wikipedia(producer)
            except Exception as e:
                print(f"[Wiki Producer] Stream error: {e}")
                print("[Wiki Producer] Reconnecting in 5 seconds...")
                time.sleep(5)

    except KeyboardInterrupt:
        print()
        print("=" * 60)
        print("[Wiki Producer] Shutting down...")
        print("=" * 60)

    producer.close()


if __name__ == "__main__":
    main()
