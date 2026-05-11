"""
Detailed Database Status Check
Shows event counts, source breakdown, and keywords
"""

from pymongo import MongoClient

def check_database():
    print("=" * 60)
    print("[DETAILED DATABASE STATUS]")
    print("=" * 60)
    print()

    try:
        client = MongoClient("mongodb://localhost:27017/")
        db = client["event_detector"]

        # Get counts
        total_events = db["processed_events"].count_documents({})
        total_keywords = db["keywords"].count_documents({})

        print(f"[TOTAL EVENTS] {total_events:,}")
        print(f"[TOTAL KEYWORDS] {total_keywords:,}")
        print()

        # Source breakdown
        print("[SOURCE BREAKDOWN]")
        sources = ["wikipedia", "news", "gdacs", "financial"]
        for source in sources:
            count = db["processed_events"].count_documents({"source_type": source})
            if count > 0:
                print(f"  {source}: {count:,} events")
        print()

        # Keywords
        print("[TOP KEYWORDS]")
        keywords = list(db["keywords"].find().sort("count", -1).limit(15))
        for kw in keywords:
            keyword = kw.get("keyword", "N/A")
            count = kw.get("count", 0)
            source = kw.get("source_type", "unknown")
            print(f"  {keyword}: {count} ({source})")
        print()

        # Latest events from each source
        print("[LATEST EVENTS BY SOURCE]")
        for source in sources:
            latest = db["processed_events"].find_one(
                {"source_type": source},
                sort=[("ingested_at", -1)]
            )
            if latest:
                title = latest.get("title", "N/A")[:50]
                keywords_list = latest.get("keywords", [])
                print(f"  {source}: {title}...")
                if keywords_list:
                    print(f"    Keywords: {', '.join(keywords_list[:3])}")
        print()

        print("=" * 60)
        print("[DASHBOARD] http://localhost:8501")
        print("=" * 60)

    except Exception as e:
        print(f"[ERROR] {e}")

if __name__ == "__main__":
    check_database()
