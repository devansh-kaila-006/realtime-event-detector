"""
MongoDB Index Setup for Performance Optimization
Creates indexes on frequently queried fields to improve performance
"""

from pymongo import MongoClient, ASCENDING, DESCENDING, TEXT
from datetime import datetime
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from config.settings import MONGO_URI, MONGO_DB


def setup_indexes():
    """Create indexes for optimized query performance"""

    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]

    print(f"[SETUP] Setting up indexes for database: {MONGO_DB}")
    print("=" * 60)

    # ============================================================
    # PROCESSED EVENTS COLLECTION
    # ============================================================

    processed_collection = db["processed_events"]

    print("[PROCESSED_EVENTS] Setting indexes...")

    # Basic query indexes
    indexes = [
        # Source type filtering (most common query)
        [("source_type", ASCENDING)],

        # Timestamp sorting and time-range queries
        [("ingested_at", DESCENDING)],
        [("timestamp", DESCENDING)],

        # Event analysis queries
        [("event_cluster", ASCENDING)],
        [("sentiment", ASCENDING)],
        [("confidence_score", DESCENDING)],

        # Compound indexes for common query patterns
        [("source_type", ASCENDING), ("timestamp", DESCENDING)],
        [("event_cluster", ASCENDING), ("confidence_score", DESCENDING)],
        [("sentiment", ASCENDING), ("timestamp", DESCENDING)],

        # Text search index
        [("title", TEXT), ("clean_text", TEXT), ("description", TEXT)]
    ]

    for index_spec in indexes:
        try:
            index_name = processed_collection.create_index(index_spec)
            print(f"  [OK] Created index: {index_name}")
        except Exception as e:
            print(f"  [ERROR] Error creating index: {e}")

    # ============================================================
    # EVENTS COLLECTION (Simplified events)
    # ============================================================

    events_collection = db["events"]

    print("\n[EVENTS] Setting indexes for 'events' collection...")

    event_indexes = [
        [("source_type", ASCENDING)],
        [("timestamp", DESCENDING)],
        [("published_at", DESCENDING)],
        [("source_type", ASCENDING), ("timestamp", DESCENDING)]
    ]

    for index_spec in event_indexes:
        try:
            index_name = events_collection.create_index(index_spec)
            print(f"  [OK] Created index: {index_name}")
        except Exception as e:
            print(f"  [ERROR] Error creating index: {e}")

    # ============================================================
    # KEYWORDS COLLECTION
    # ============================================================

    keywords_collection = db["keywords"]

    print("\n[KEYWORDS] Setting indexes for 'keywords' collection...")

    keyword_indexes = [
        [("keyword", ASCENDING)],
        [("source_type", ASCENDING)],
        [("keyword", ASCENDING), ("source_type", ASCENDING)]
    ]

    for index_spec in keyword_indexes:
        try:
            index_name = keywords_collection.create_index(index_spec)
            print(f"  [OK] Created index: {index_name}")
        except Exception as e:
            print(f"  [ERROR] Error creating index: {e}")

    # ============================================================
    # DISPLAY INDEX INFORMATION
    # ============================================================

    print("\n" + "=" * 60)
    print("[SUMMARY] Index Summary:")
    print("=" * 60)

    collections_to_check = ["processed_events", "events", "keywords"]

    for collection_name in collections_to_check:
        collection = db[collection_name]
        indexes = collection.list_indexes()

        print(f"\n[COLLECTION] {collection_name}:")
        for index in indexes:
            print(f"  - {index['name']}: {index['key']}")

    print("\n" + "=" * 60)
    print("[SUCCESS] Index setup complete!")
    print("=" * 60)

    # ============================================================
    # PERFORMANCE TIPS
    # ============================================================

    print("\n[PERFORMANCE TIPS]")
    print("  * Monitor slow queries with: db.setProfilingLevel(1, {slowms: 100})")
    print("  * Check index usage with: db.collection.explain('executionStats').find(...)")
    print("  * Rebuild indexes periodically for optimal performance")
    print("  * Consider sharding if collections grow beyond 100GB")


if __name__ == "__main__":
    setup_indexes()