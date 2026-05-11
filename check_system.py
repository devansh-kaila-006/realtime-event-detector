"""
Quick System Status Check
Shows database stats and system status
"""

from pymongo import MongoClient
from datetime import datetime

def check_system():
    print("=" * 60)
    print("[SYSTEM STATUS CHECK]")
    print("=" * 60)
    print()

    try:
        # Connect to MongoDB
        client = MongoClient("mongodb://localhost:27017/")
        db = client["event_detector"]

        # Check collections
        total_events = db["processed_events"].count_documents({})
        simplified_events = db["events"].count_documents({})
        keywords = db["keywords"].count_documents({})

        print(f"[DATABASE] Event Counts:")
        print(f"  Total processed events: {total_events}")
        print(f"  Simplified events: {simplified_events}")
        print(f"  Keywords: {keywords}")
        print()

        if total_events > 0:
            print("[DATA FLOW] Active - Data is flowing through system!")
            print()

            # Get source breakdown
            print("[SOURCE BREAKDOWN]")
            for source in ["wikipedia", "news", "gdacs", "financial"]:
                count = db["processed_events"].count_documents({"source_type": source})
                if count > 0:
                    print(f"  {source}: {count} events")

            # Get latest event
            latest = db["processed_events"].find_one(sort=[("ingested_at", -1)])
            if latest:
                print()
                print("[LATEST EVENT]")
                print(f"  Title: {latest.get('title', 'N/A')[:60]}...")
                print(f"  Source: {latest.get('source_type', 'N/A')}")
                print(f"  Time: {latest.get('ingested_at', 'N/A')}")
        else:
            print("[DATA FLOW] Waiting - No events yet")
            print()
            print("[INFO] Possible reasons:")
            print("  - Producers just started (need time to fetch data)")
            print("  - APIs may have rate limits")
            print("  - Spark consumer still initializing")
            print()
            print("[STATUS] All components running, waiting for first events...")

        print()
        print("=" * 60)
        print("[DASHBOARD] http://localhost:8501")
        print("[KAFKA UI] http://localhost:8080")
        print("=" * 60)

    except Exception as e:
        print(f"[ERROR] {e}")
        print("[INFO] Make sure MongoDB is running on localhost:27027")

if __name__ == "__main__":
    check_system()
