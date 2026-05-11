"""
News API → Kafka producer.
Polls NewsAPI every 60 seconds, deduplicates by title,
and publishes clean JSON messages to the news_stream topic.

FIX for empty/inconsistent stream:
- Added retry with exponential backoff
- Added HTTP status validation
- Added max deduplication cache size (prevents unbounded memory growth)
- Added multiple category fetching to increase article volume
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

# Fetch multiple categories for richer event detection
NEWS_ENDPOINTS = [
    f"https://newsapi.org/v2/top-headlines?language=en&pageSize=20&apiKey={NEWS_API_KEY}",
    f"https://newsapi.org/v2/top-headlines?language=en&category=technology&pageSize=10&apiKey={NEWS_API_KEY}",
    f"https://newsapi.org/v2/top-headlines?language=en&category=science&pageSize=10&apiKey={NEWS_API_KEY}",
    f"https://newsapi.org/v2/top-headlines?language=en&category=health&pageSize=10&apiKey={NEWS_API_KEY}",
]

POLL_INTERVAL = 60  # 1 minute - more frequent updates for better real-time detection
MAX_CACHE_SIZE = 2000   # prevent unbounded memory growth
MAX_RETRIES = 3


def create_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda x: json.dumps(x).encode("utf-8"),
        retries=5,
        acks="all"
    )


def fetch_articles(url: str, retries: int = MAX_RETRIES) -> list:
    """Fetch articles with exponential backoff retry."""
    for attempt in range(retries):
        try:
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                data = response.json()
                if data.get("status") == "ok":
                    return data.get("articles", [])
                else:
                    print(f"[News Producer] API error: {data.get('message')}")
                    return []
            elif response.status_code == 429:
                wait = 2 ** attempt * 10
                print(f"[News Producer] Rate limited. Waiting {wait}s...")
                time.sleep(wait)
            else:
                print(f"[News Producer] HTTP {response.status_code} for {url}")
                return []
        except requests.exceptions.Timeout:
            print(f"[News Producer] Timeout (attempt {attempt+1})")
            time.sleep(2 ** attempt)
        except requests.exceptions.ConnectionError as e:
            print(f"[News Producer] Connection error: {e}")
            time.sleep(2 ** attempt)
    return []


def build_message(article: dict) -> dict:
    return {
        "title":        article.get("title"),
        "description":  article.get("description"),
        "content":      article.get("content"),
        "source":       article.get("source", {}).get("name"),
        "author":       article.get("author"),
        "published_at": article.get("publishedAt"),
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

        for url in NEWS_ENDPOINTS:
            # Tag category from URL for downstream use
            category = "general"
            for cat in ["technology", "science", "health", "business"]:
                if cat in url:
                    category = cat
                    break

            articles = fetch_articles(url)

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