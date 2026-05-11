"""
MongoDB writer — handles all persistence for the event detection system.
Creates TTL indexes to automatically expire old raw events after 7 days.
"""

from datetime import datetime
from pymongo import MongoClient, ASCENDING, DESCENDING
from pymongo.errors import ConnectionFailure
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from config.settings import MONGO_URI, MONGO_DB, MONGO_COLLECTION_EVENTS, MONGO_COLLECTION_KEYWORDS


class MongoWriter:

    def __init__(self):
        try:
            self.client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
            self.client.admin.command("ping")
            self.db = self.client[MONGO_DB]
            self._ensure_indexes()
            print("[MongoDB] Connected successfully.")
        except ConnectionFailure as e:
            print(f"[MongoDB] Connection failed: {e}")
            print("[MongoDB] Events will not be persisted. Start MongoDB and restart.")
            self.db = None

    def _ensure_indexes(self):
        events = self.db[MONGO_COLLECTION_EVENTS]
        # TTL index: auto-delete raw events after 7 days
        events.create_index("ingested_at", expireAfterSeconds=7 * 24 * 3600)
        # Query indexes
        events.create_index([("event_type", ASCENDING)])
        events.create_index([("confidence",  DESCENDING)])
        events.create_index([("source_type", ASCENDING)])

        keywords = self.db[MONGO_COLLECTION_KEYWORDS]
        keywords.create_index([("keyword", ASCENDING)], unique=True)
        keywords.create_index([("count",   DESCENDING)])

    def write_event(self, doc: dict):
        if self.db is None:
            return
        try:
            doc["ingested_at"] = datetime.utcnow()
            # Convert any non-serialisable types
            for k, v in list(doc.items()):
                if not isinstance(v, (str, int, float, bool, list, dict, type(None))):
                    doc[k] = str(v)
            self.db[MONGO_COLLECTION_EVENTS].insert_one(doc)
        except Exception as e:
            print(f"[MongoDB] write_event error: {e}")

    def update_keyword_trends(self, counter: dict):
        """Upsert keyword counts for trending keyword view."""
        if self.db is None:
            return
        try:
            for keyword, count in counter.items():
                self.db[MONGO_COLLECTION_KEYWORDS].update_one(
                    {"keyword": keyword},
                    {"$inc": {"count": count}, "$set": {"last_seen": datetime.utcnow()}},
                    upsert=True
                )
        except Exception as e:
            print(f"[MongoDB] update_keyword_trends error: {e}")

    def get_recent_events(self, limit: int = 50, event_type: str = None) -> list:
        if self.db is None:
            return []
        query = {}
        if event_type and event_type != "all":
            query["event_type"] = event_type
        return list(
            self.db[MONGO_COLLECTION_EVENTS]
            .find(query, {"_id": 0})
            .sort("ingested_at", DESCENDING)
            .limit(limit)
        )

    def get_trending_keywords(self, limit: int = 20) -> list:
        if self.db is None:
            return []
        return list(
            self.db[MONGO_COLLECTION_KEYWORDS]
            .find({}, {"_id": 0})
            .sort("count", DESCENDING)
            .limit(limit)
        )

    def get_event_type_counts(self) -> dict:
        if self.db is None:
            return {}
        pipeline = [
            {"$group": {"_id": "$event_type", "count": {"$sum": 1}}},
            {"$sort":  {"count": -1}}
        ]
        results = self.db[MONGO_COLLECTION_EVENTS].aggregate(pipeline)
        return {r["_id"]: r["count"] for r in results}

    def get_top_countries(self, limit: int = 10) -> list:
        """Extract most-mentioned countries/locations from entity JSON."""
        if self.db is None:
            return []
        pipeline = [
            {"$match": {"entities": {"$exists": True}}},
            {"$project": {"countries": {"$function": {
                "body": """function(e) {
                    try { return JSON.parse(e).countries || []; }
                    catch(err) { return []; }
                }""",
                "args": ["$entities"],
                "lang": "js"
            }}}},
            {"$unwind": "$countries"},
            {"$group": {"_id": "$countries", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": limit}
        ]
        try:
            return list(self.db[MONGO_COLLECTION_EVENTS].aggregate(pipeline))
        except Exception:
            # JS aggregation may not be available on all MongoDB versions
            return []