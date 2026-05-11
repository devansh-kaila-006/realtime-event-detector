"""
End-to-End Testing Suite for Real-Time Event Detection System
Tests all components: producers, consumers, NLP pipeline, and dashboard
"""

import json
import time
import sys
import os
from pymongo import MongoClient
from datetime import datetime, timedelta

sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from config.settings import (
    MONGO_URI, MONGO_DB, KAFKA_BOOTSTRAP,
    KAFKA_WIKI_TOPIC, KAFKA_NEWS_TOPIC, KAFKA_GDACS_TOPIC, KAFKA_FINANCIAL_TOPIC
)


class SystemTester:
    """Comprehensive testing suite for the event detection system"""

    def __init__(self):
        self.mongo_client = MongoClient(MONGO_URI)
        self.db = self.mongo_client[MONGO_DB]
        self.results = []

    def log_test(self, test_name, passed, message, duration=0):
        """Log test result"""
        result = {
            "test_name": test_name,
            "passed": passed,
            "message": message,
            "duration": duration,
            "timestamp": datetime.utcnow().isoformat()
        }
        self.results.append(result)

        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"{status} | {test_name} | {message} ({duration:.2f}s)")

    def test_mongodb_connection(self):
        """Test MongoDB connection"""
        start_time = time.time()
        try:
            # Test basic connection
            self.mongo_client.admin.command('ping')

            # Check database exists
            db_names = self.mongo_client.list_database_names()
            if MONGO_DB in db_names:
                duration = time.time() - start_time
                self.log_test("MongoDB Connection", True, "Connected successfully", duration)
                return True
            else:
                duration = time.time() - start_time
                self.log_test("MongoDB Connection", False, f"Database '{MONGO_DB}' not found", duration)
                return False

        except Exception as e:
            duration = time.time() - start_time
            self.log_test("MongoDB Connection", False, f"Connection failed: {e}", duration)
            return False

    def test_collections_exist(self):
        """Test that required collections exist"""
        start_time = time.time()
        required_collections = ["processed_events", "events", "keywords"]

        try:
            collection_names = self.db.list_collection_names()
            missing = [c for c in required_collections if c not in collection_names]

            if missing:
                duration = time.time() - start_time
                self.log_test("Collections Exist", False, f"Missing collections: {missing}", duration)
                return False
            else:
                duration = time.time() - start_time
                self.log_test("Collections Exist", True, f"All {len(required_collections)} collections exist", duration)
                return True

        except Exception as e:
            duration = time.time() - start_time
            self.log_test("Collections Exist", False, f"Error checking collections: {e}", duration)
            return False

    def test_indexes_exist(self):
        """Test that performance indexes are created"""
        start_time = time.time()
        try:
            processed_collection = self.db["processed_events"]
            indexes = processed_collection.list_indexes()

            index_names = [idx['name'] for idx in indexes]

            # Check for key indexes
            required_indexes = ["source_type_1", "ingested_at_-1", "event_cluster_1", "sentiment_1"]
            missing = [idx for idx in required_indexes if idx not in index_names]

            if missing:
                duration = time.time() - start_time
                self.log_test("Performance Indexes", False, f"Missing indexes: {missing}", duration)
                return False
            else:
                duration = time.time() - start_time
                self.log_test("Performance Indexes", True, f"All {len(required_indexes)} key indexes exist", duration)
                return True

        except Exception as e:
            duration = time.time() - start_time
            self.log_test("Performance Indexes", False, f"Error checking indexes: {e}", duration)
            return False

    def test_data_in_collections(self):
        """Test that data is being processed and stored"""
        start_time = time.time()
        try:
            total_events = self.db["processed_events"].count_documents({})

            if total_events == 0:
                duration = time.time() - start_time
                self.log_test("Data in Collections", False, "No events found in database", duration)
                return False

            # Check different source types
            sources = ["wikipedia", "news", "gdacs", "financial"]
            source_counts = {}

            for source in sources:
                count = self.db["processed_events"].count_documents({"source_type": source})
                source_counts[source] = count

            duration = time.time() - start_time
            message = f"Total events: {total_events} | Sources: {source_counts}"
            self.log_test("Data in Collections", True, message, duration)
            return True

        except Exception as e:
            duration = time.time() - start_time
            self.log_test("Data in Collections", False, f"Error checking data: {e}", duration)
            return False

    def test_nlp_enrichment(self):
        """Test that NLP fields are being populated"""
        start_time = time.time()
        try:
            # Get a sample event
            sample_event = self.db["processed_events"].find_one({})

            if not sample_event:
                duration = time.time() - start_time
                self.log_test("NLP Enrichment", False, "No events to test NLP fields", duration)
                return False

            # Check for NLP fields
            required_fields = ["clean_text", "words", "sentiment", "event_cluster", "confidence_score"]
            missing_fields = [field for field in required_fields if field not in sample_event]

            if missing_fields:
                duration = time.time() - start_time
                self.log_test("NLP Enrichment", False, f"Missing NLP fields: {missing_fields}", duration)
                return False

            duration = time.time() - start_time
            message = f"NLP fields present | Sentiment: {sample_event.get('sentiment')} | Cluster: {sample_event.get('event_cluster')}"
            self.log_test("NLP Enrichment", True, message, duration)
            return True

        except Exception as e:
            duration = time.time() - start_time
            self.log_test("NLP Enrichment", False, f"Error testing NLP: {e}", duration)
            return False

    def test_data_freshness(self):
        """Test that data is being processed in real-time"""
        start_time = time.time()
        try:
            # Check for recent events (last 10 minutes)
            recent_cutoff = datetime.utcnow() - timedelta(minutes=10)
            recent_events = self.db["processed_events"].count_documents({
                "ingested_at": {"$gte": recent_cutoff}
            })

            if recent_events == 0:
                duration = time.time() - start_time
                self.log_test("Data Freshness", False, "No recent events (last 10 minutes)", duration)
                return False

            duration = time.time() - start_time
            self.log_test("Data Freshness", True, f"{recent_events} events in last 10 minutes", duration)
            return True

        except Exception as e:
            duration = time.time() - start_time
            self.log_test("Data Freshness", False, f"Error checking freshness: {e}", duration)
            return False

    def test_performance_metrics(self):
        """Test system performance metrics"""
        start_time = time.time()
        try:
            # Test query performance
            query_start = time.time()

            # Complex aggregation query
            pipeline = [
                {"$match": {"ingested_at": {"$gte": datetime.utcnow() - timedelta(hours=1)}}},
                {"$group": {
                    "_id": "$source_type",
                    "count": {"$sum": 1},
                    "avg_confidence": {"$avg": "$confidence_score"}
                }}
            ]

            results = list(self.db["processed_events"].aggregate(pipeline))
            query_duration = time.time() - query_start

            # Query should complete in less than 1 second
            if query_duration > 1.0:
                duration = time.time() - start_time
                self.log_test("Query Performance", False, f"Query too slow: {query_duration:.2f}s", duration)
                return False

            duration = time.time() - start_time
            self.log_test("Query Performance", True, f"Queries responsive ({query_duration:.3f}s)", duration)
            return True

        except Exception as e:
            duration = time.time() - start_time
            self.log_test("Query Performance", False, f"Error testing performance: {e}", duration)
            return False

    def test_kafka_topics(self):
        """Test that Kafka topics are configured"""
        start_time = time.time()
        try:
            from kafka import KafkaAdminClient
            from kafka.errors import KafkaError

            admin_client = KafkaAdminClient(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                client_id='test_client'
            )

            # List topics
            topics = admin_client.list_topics()

            required_topics = [
                KAFKA_WIKI_TOPIC,
                KAFKA_NEWS_TOPIC,
                KAFKA_GDACS_TOPIC,
                KAFKA_FINANCIAL_TOPIC
            ]

            missing_topics = [topic for topic in required_topics if topic not in topics]

            if missing_topics:
                duration = time.time() - start_time
                self.log_test("Kafka Topics", False, f"Missing topics: {missing_topics}", duration)
                return False

            duration = time.time() - start_time
            self.log_test("Kafka Topics", True, f"All {len(required_topics)} topics exist", duration)
            return True

        except Exception as e:
            duration = time.time() - start_time
            self.log_test("Kafka Topics", False, f"Error checking Kafka: {e}", duration)
            return False

    def test_geographic_data(self):
        """Test geographic data processing"""
        start_time = time.time()
        try:
            # Check for events with coordinates
            geo_events = self.db["processed_events"].count_documents({
                "latitude": {"$exists": True},
                "longitude": {"$exists": True}
            })

            duration = time.time() - start_time

            if geo_events > 0:
                self.log_test("Geographic Data", True, f"{geo_events} events with coordinates", duration)
                return True
            else:
                self.log_test("Geographic Data", False, "No events with geographic data", duration)
                return False

        except Exception as e:
            duration = time.time() - start_time
            self.log_test("Geographic Data", False, f"Error testing geographic data: {e}", duration)
            return False

    def run_all_tests(self):
        """Run all tests and generate report"""
        print("=" * 80)
        print("🧪 REAL-TIME EVENT DETECTION SYSTEM - END-TO-END TESTING")
        print("=" * 80)
        print(f"📅 Started: {datetime.utcnow().isoformat()}")
        print()

        # Run all tests
        self.test_mongodb_connection()
        self.test_collections_exist()
        self.test_indexes_exist()
        self.test_kafka_topics()
        self.test_data_in_collections()
        self.test_nlp_enrichment()
        self.test_geographic_data()
        self.test_data_freshness()
        self.test_performance_metrics()

        # Generate summary
        print()
        print("=" * 80)
        print("📊 TEST SUMMARY")
        print("=" * 80)

        total_tests = len(self.results)
        passed_tests = sum(1 for r in self.results if r['passed'])
        failed_tests = total_tests - passed_tests

        print(f"Total Tests: {total_tests}")
        print(f"✅ Passed: {passed_tests}")
        print(f"❌ Failed: {failed_tests}")
        print(f"Success Rate: {(passed_tests/total_tests*100):.1f}%")
        print()

        if failed_tests > 0:
            print("❌ FAILED TESTS:")
            for result in self.results:
                if not result['passed']:
                    print(f"  • {result['test_name']}: {result['message']}")
            print()

        # Calculate total duration
        total_duration = sum(r['duration'] for r in self.results)
        print(f"⏱️ Total Testing Time: {total_duration:.2f}s")
        print(f"📅 Completed: {datetime.utcnow().isoformat()}")
        print("=" * 80)

        return failed_tests == 0


def main():
    """Main testing entry point"""
    tester = SystemTester()
    success = tester.run_all_tests()

    # Save test results to file
    results_file = "test_results.json"
    with open(results_file, 'w') as f:
        json.dump(tester.results, f, indent=2)

    print(f"\n📁 Test results saved to: {results_file}")

    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())