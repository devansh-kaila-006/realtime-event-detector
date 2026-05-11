"""
Currents API → Kafka producer.
Polls Currents every 30 seconds, deduplicates by title,
and publishes clean JSON messages to the news_stream topic.
"""

import json
import time
import requests
from collections import deque
from kafka import KafkaProducer
from kafka.errors import KafkaError

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from config.settings import KAFKA_BOOTSTRAP, KAFKA_NEWS_TOPIC, NEWS_API_KEY

CURRENTS_API_URL = "https://api.currentsapi.services/v1/latest-news"
CATEGORIES = ["general", "technology", "science", "health"]

POLL_INTERVAL = 30  # 30 seconds
MAX_CACHE_SIZE = 2000   # prevent unbounded memory growth
MAX_RETRIES = 3


def create_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda x: json.dumps(x).encode("utf-8"),
        retries=5,
        acks="all"
    )


def fetch_articles(category: str, retries: int = MAX_RETRIES) -> list:
    """Fetch articles with exponential backoff retry."""
    params = {
        "language": "en",
        "category": category,
        "apiKey": NEWS_API_KEY
    }

    for attempt in range(retries):
        try:
            response = requests.get(CURRENTS_API_URL, params=params, timeout=10)
            if response.status_code == 200:
                data = response.json()
                if data.get("status") == "ok" or "news" in data:
                    return data.get("news", [])
                else:
                    print(f"[News Producer] API error: {data.get('message')}")
                    return []
            elif response.status_code == 429:
                wait = 2 ** attempt * 10
                print(f"[News Producer] Rate limited. Waiting {wait}s...")
                time.sleep(wait)
            else:
                print(f"[News Producer] HTTP {response.status_code} for category={category}")
                return []
        except requests.exceptions.Timeout:
            print(f"[News Producer] Timeout (attempt {attempt+1})")
            time.sleep(2 ** attempt)
        except requests.exceptions.ConnectionError as e:
            print(f"[News Producer] Connection error: {e}")
            time.sleep(2 ** attempt)
    return []


def build_message(article: dict) -> dict:
    source = article.get("author")
    if isinstance(source, list):
        source = ", ".join(source[:2]) if source else None

    return {
        "title":        article.get("title"),
        "description":  article.get("description") or article.get("summary"),
        "content":      article.get("description") or article.get("summary"),
        "source":       source,
        "author":       source,
        "published_at": article.get("published"),
        "url":          article.get("url"),
        "category":     article.get("_category", "general"),
    }


def main():
    producer = create_producer()

    # Use deque for O(1) cache eviction
    sent_titles = deque(maxlen=MAX_CACHE_SIZE)
    sent_set = set()

    print("\n[News Producer] Starting news stream...\n")

    while True:
        batch_count = 0

        for category in CATEGORIES:
            articles = fetch_articles(category)

            for article in articles:
                title = article.get("title")
                if not title or title in sent_set:
                    continue

                # Evict oldest if cache is full
                if len(sent_titles) == MAX_CACHE_SIZE:
                    old = sent_titles[0]  # leftmost = oldest
                    sent_set.discard(old)

                sent_titles.append(title)
                sent_set.add(title)

                article["_category"] = category
                message = build_message(article)

                try:
                    future = producer.send(KAFKA_NEWS_TOPIC, value=message)
                    future.get(timeout=5)
                    batch_count += 1
                    print(f"[News] {category:12s} | {title[:60]}")
                except KafkaError as e:
                    print(f"[News Producer] Kafka error: {e}")

        print(f"\n[News Producer] Sent {batch_count} new articles. Waiting {POLL_INTERVAL}s...\n")
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
