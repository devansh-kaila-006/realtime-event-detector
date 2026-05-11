"""
Performance Validation and Benchmarking Tool
Validates system performance and provides optimization recommendations
"""

import time
import psutil
import sys
import os
from pymongo import MongoClient, ASCENDING, DESCENDING
from datetime import datetime, timedelta

sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from config.settings import MONGO_URI, MONGO_DB


class PerformanceValidator:
    """Validates system performance and provides recommendations"""

    def __init__(self):
        self.mongo_client = MongoClient(MONGO_URI)
        self.db = self.mongo_client[MONGO_DB]
        self.recommendations = []

    def log_recommendation(self, category, issue, recommendation, priority="medium"):
        """Log a performance recommendation"""
        rec = {
            "category": category,
            "issue": issue,
            "recommendation": recommendation,
            "priority": priority,
            "timestamp": datetime.utcnow().isoformat()
        }
        self.recommendations.append(rec)

    def check_system_resources(self):
        """Check system resource usage"""
        print("🖥️  Checking System Resources...")

        # CPU usage
        cpu_percent = psutil.cpu_percent(interval=1)
        print(f"   CPU Usage: {cpu_percent}%")

        if cpu_percent > 80:
            self.log_recommendation(
                "System Resources",
                f"High CPU usage ({cpu_percent}%)",
                "Consider reducing Spark workers or increasing CPU capacity",
                "high"
            )

        # Memory usage
        memory = psutil.virtual_memory()
        memory_percent = memory.percent
        print(f"   Memory Usage: {memory_percent}% ({memory.used / (1024**3):.1f}GB / {memory.total / (1024**3):.1f}GB)")

        if memory_percent > 85:
            self.log_recommendation(
                "System Resources",
                f"High memory usage ({memory_percent}%)",
                "Reduce event limits, increase memory, or implement data archiving",
                "high"
            )

        # Disk usage
        disk = psutil.disk_usage('/')
        disk_percent = disk.percent
        print(f"   Disk Usage: {disk_percent}% ({disk.used / (1024**3):.1f}GB / {disk.total / (1024**3):.1f}GB)")

        if disk_percent > 85:
            self.log_recommendation(
                "System Resources",
                f"High disk usage ({disk_percent}%)",
                "Implement data archival or increase storage capacity",
                "high"
            )

        print()

    def check_query_performance(self):
        """Benchmark database query performance"""
        print("🔍 Checking Query Performance...")

        collection = self.db["processed_events"]

        # Test 1: Simple query with index
        start = time.time()
        result = collection.find_one({"source_type": "wikipedia"})
        duration = time.time() - start

        print(f"   Simple indexed query: {duration*1000:.2f}ms")

        if duration > 0.1:  # 100ms
            self.log_recommendation(
                "Query Performance",
                f"Slow simple query ({duration*1000:.2f}ms)",
                "Check indexes are properly created and used",
                "medium"
            )

        # Test 2: Complex aggregation
        start = time.time()
        pipeline = [
            {"$match": {"ingested_at": {"$gte": datetime.utcnow() - timedelta(hours=1)}}},
            {"$group": {
                "_id": "$source_type",
                "count": {"$sum": 1},
                "avg_confidence": {"$avg": "$confidence_score"}
            }}
        ]
        results = list(collection.aggregate(pipeline))
        duration = time.time() - start

        print(f"   Complex aggregation query: {duration*1000:.2f}ms")

        if duration > 1.0:  # 1 second
            self.log_recommendation(
                "Query Performance",
                f"Slow aggregation ({duration*1000:.2f}ms)",
                "Add compound indexes for common query patterns",
                "medium"
            )

        # Test 3: Full-text search
        start = time.time()
        results = collection.find({"$text": {"$search": "earthquake"}}).limit(10)
        list(results)
        duration = time.time() - start

        print(f"   Full-text search query: {duration*1000:.2f}ms")

        if duration > 0.5:  # 500ms
            self.log_recommendation(
                "Query Performance",
                f"Slow text search ({duration*1000:.2f}ms)",
                "Optimize text indexes or reduce search scope",
                "low"
            )

        print()

    def check_data_volume(self):
        """Analyze data volume and growth patterns"""
        print("📊 Analyzing Data Volume...")

        collection = self.db["processed_events"]

        # Total documents
        total_docs = collection.estimated_document_count()
        print(f"   Total events: {total_docs:,}")

        if total_docs > 1000000:  # 1 million
            self.log_recommendation(
                "Data Volume",
                f"Large dataset ({total_docs:,} events)",
                "Implement data archival or partitioning",
                "medium"
            )

        # Collection size
        stats = self.db.command("collstats", "processed_events")
        size_mb = stats['size'] / (1024**2)
        print(f"   Collection size: {size_mb:.2f}MB")

        if size_mb > 1000:  # 1GB
            self.log_recommendation(
                "Data Volume",
                f"Large collection size ({size_mb:.2f}MB)",
                "Consider data archiving or compression",
                "medium"
            )

        # Average document size
        avg_doc_size = stats['avgObjSize']
        print(f"   Average document size: {avg_doc_size / 1024:.2f}KB")

        if avg_doc_size > 10240:  # 10KB
            self.log_recommendation(
                "Data Volume",
                f"Large document size ({avg_doc_size / 1024:.2f}KB)",
                "Optimize document structure or remove unnecessary fields",
                "low"
            )

        # Growth rate (last hour)
        hour_ago = datetime.utcnow() - timedelta(hours=1)
        recent_count = collection.count_documents({"ingested_at": {"$gte": hour_ago}})
        print(f"   Events per hour: {recent_count}")

        if recent_count > 10000:  # 10k per hour
            self.log_recommendation(
                "Data Volume",
                f"High ingestion rate ({recent_count:,} events/hour)",
                "Scale processing infrastructure or implement sampling",
                "high"
            )

        print()

    def check_index_usage(self):
        """Verify index usage and effectiveness"""
        print("📈 Checking Index Usage...")

        collection = self.db["processed_events"]

        # Get index information
        indexes = collection.list_indexes()
        print(f"   Total indexes: {len(list(indexes))}")

        # Check for critical indexes
        critical_indexes = [
            "source_type_1",
            "ingested_at_-1",
            "event_cluster_1",
            "sentiment_1"
        ]

        existing_indexes = [idx['name'] for idx in collection.list_indexes()]
        missing = [idx for idx in critical_indexes if idx not in existing_indexes]

        if missing:
            self.log_recommendation(
                "Index Usage",
                f"Missing critical indexes: {missing}",
                "Run database/setup_indexes.py to create performance indexes",
                "high"
            )
        else:
            print(f"   ✅ All critical indexes present")

        print()

    def check_data_freshness(self):
        """Analyze data freshness and processing lag"""
        print("⏰ Checking Data Freshness...")

        collection = self.db["processed_events"]

        # Latest event time
        latest = collection.find_one(sort=[("ingested_at", DESCENDING)])
        if latest and latest.get('ingested_at'):
            latest_time = latest['ingested_at']
            if isinstance(latest_time, str):
                latest_time = datetime.fromisoformat(latest_time.replace('Z', '+00:00'))

            lag = datetime.utcnow() - latest_time
            lag_seconds = lag.total_seconds()

            print(f"   Processing lag: {lag_seconds:.1f} seconds")

            if lag_seconds > 60:  # 1 minute
                self.log_recommendation(
                    "Data Freshness",
                    f"High processing lag ({lag_seconds:.1f}s)",
                    "Check Spark consumer performance and Kafka lag",
                    "high"
                )
            else:
                print(f"   ✅ Processing lag acceptable")

        # Recent activity
        minute_ago = datetime.utcnow() - timedelta(minutes=1)
        recent_count = collection.count_documents({"ingested_at": {"$gte": minute_ago}})
        print(f"   Events in last minute: {recent_count}")

        if recent_count == 0:
            self.log_recommendation(
                "Data Freshness",
                "No recent events detected",
                "Verify producers and Spark consumer are running",
                "high"
            )

        print()

    def check_memory_leaks(self):
        """Check for potential memory issues"""
        print("🧠 Checking Memory Health...")

        process = psutil.Process()
        memory_info = process.memory_info()

        print(f"   Process memory: {memory_info.rss / (1024**2):.1f}MB")
        print(f"   Memory percent: {process.memory_percent()}%")

        # Check for memory growth (simple heuristic)
        if process.memory_percent() > 10:  # Using > 10% of system memory
            self.log_recommendation(
                "Memory Health",
                f"High memory usage ({process.memory_percent()}%)",
                "Check for memory leaks, reduce batch sizes, or restart services",
                "medium"
            )

        print()

    def generate_report(self):
        """Generate performance report with recommendations"""
        print("=" * 80)
        print("📋 PERFORMANCE VALIDATION REPORT")
        print("=" * 80)
        print()

        # Priority breakdown
        high_priority = [r for r in self.recommendations if r['priority'] == 'high']
        medium_priority = [r for r in self.recommendations if r['priority'] == 'medium']
        low_priority = [r for r in self.recommendations if r['priority'] == 'low']

        print(f"🎯 Total Recommendations: {len(self.recommendations)}")
        print(f"   🔴 High Priority: {len(high_priority)}")
        print(f"   🟡 Medium Priority: {len(medium_priority)}")
        print(f"   🟢 Low Priority: {len(low_priority)}")
        print()

        if high_priority:
            print("🔴 HIGH PRIORITY RECOMMENDATIONS:")
            for i, rec in enumerate(high_priority, 1):
                print(f"{i}. [{rec['category']}] {rec['issue']}")
                print(f"   💡 {rec['recommendation']}")
            print()

        if medium_priority:
            print("🟡 MEDIUM PRIORITY RECOMMENDATIONS:")
            for i, rec in enumerate(medium_priority, 1):
                print(f"{i}. [{rec['category']}] {rec['issue']}")
                print(f"   💡 {rec['recommendation']}")
            print()

        if low_priority:
            print("🟢 LOW PRIORITY RECOMMENDATIONS:")
            for i, rec in enumerate(low_priority, 1):
                print(f"{i}. [{rec['category']}] {rec['issue']}")
                print(f"   💡 {rec['recommendation']}")
            print()

        if not self.recommendations:
            print("✅ No performance issues detected! System is running optimally.")
            print()

        print("=" * 80)
        print("📈 PERFORMANCE OPTIMIZATION TIPS")
        print("=" * 80)
        print()

        tips = [
            "1. Monitor query performance regularly and adjust indexes",
            "2. Implement data archival for events older than 30 days",
            "3. Use Redis caching layer for frequently accessed data",
            "4. Scale Kafka partitions based on throughput requirements",
            "5. Optimize Spark batch sizes for your hardware",
            "6. Use pagination in dashboard to reduce memory usage",
            "7. Implement connection pooling for database access",
            "8. Monitor system resources and set up alerts",
            "9. Regular maintenance: index rebuilds, statistics update",
            "10. Load test before production deployment"
        ]

        for tip in tips:
            print(tip)

        print()
        print("=" * 80)

    def run_validation(self):
        """Run complete performance validation"""
        print("=" * 80)
        print("🚀 REAL-TIME EVENT DETECTION SYSTEM - PERFORMANCE VALIDATION")
        print("=" * 80)
        print(f"📅 Started: {datetime.utcnow().isoformat()}")
        print()

        # Run all checks
        self.check_system_resources()
        self.check_query_performance()
        self.check_data_volume()
        self.check_index_usage()
        self.check_data_freshness()
        self.check_memory_leaks()

        # Generate report
        self.generate_report()

        return len(self.recommendations)


def main():
    """Main validation entry point"""
    validator = PerformanceValidator()
    issues_found = validator.run_validation()

    # Save recommendations to file
    import json
    recommendations_file = "performance_recommendations.json"
    with open(recommendations_file, 'w') as f:
        json.dump(validator.recommendations, f, indent=2)

    print(f"\n📁 Recommendations saved to: {recommendations_file}")
    print(f"📊 Performance issues found: {issues_found}")

    return 0 if issues_found == 0 else 1


if __name__ == "__main__":
    sys.exit(main())