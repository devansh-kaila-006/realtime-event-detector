"""
MongoDB Database Reset Script
Clears all collections to start fresh with balanced data
"""

from pymongo import MongoClient
from datetime import datetime

print("=" * 60)
print("[DATABASE RESET] Clearing all MongoDB collections")
print("=" * 60)
print()

try:
    # Connect to MongoDB
    client = MongoClient("mongodb://localhost:27017/")
    db = client["event_detector"]

    collections_to_clear = [
        "processed_events",
        "events",
        "keywords"
    ]

    for collection_name in collections_to_clear:
        collection = db[collection_name]

        # Count documents before deletion
        count_before = collection.estimated_document_count()

        if count_before > 0:
            print(f"[CLEARING] {collection_name}: {count_before:,} documents")

            # Delete all documents
            result = collection.delete_many({})

            print(f"  [OK] Deleted: {result.deleted_count:,} documents")
        else:
            print(f"[INFO] {collection_name}: Already empty")

    print()
    print("=" * 60)
    print("[SUCCESS] All collections cleared!")
    print("=" * 60)
    print()
    print("[NEXT STEPS]")
    print("1. All MongoDB collections are now empty")
    print("2. Producers will start sending fresh data")
    print("3. Wikipedia producer now uses 10% sampling")
    print("4. System will repopulate with balanced data")
    print()
    print(f"[TIMESTAMP] Reset completed at: {datetime.utcnow()}")
    print()

except Exception as e:
    print(f"[ERROR] Failed to clear databases: {e}")
    print()
    print("[INFO] Make sure MongoDB is running on localhost:27017")
