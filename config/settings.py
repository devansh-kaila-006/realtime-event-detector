KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_WIKI_TOPIC = "wiki_stream"
KAFKA_NEWS_TOPIC = "news_stream"
KAFKA_GDACS_TOPIC = "gdacs_stream"
KAFKA_FINANCIAL_TOPIC = "financial_stream"

MONGO_URI = "mongodb://localhost:27017"
MONGO_DB = "event_detector"
MONGO_COLLECTION_EVENTS = "events"
MONGO_COLLECTION_KEYWORDS = "keywords"

SPARK_APP_NAME = "RealTimeGlobalEventDetector"
SPARK_CHECKPOINT = "C:/tmp/checkpoints"

NEWS_API_KEY = "aElUiLCAfamjZEGIIzlmmXT8wPJ_3awZ_Kqvl4JZJNZQDD37"

# ─── Financial Data ────────────────────────────────────────────────
FINNHUB_API_KEY = "d80jpapr01qt5k5v7tqgd80jpapr01qt5k5v7tr0"
FINNHUB_API_BASE = "https://finnhub.io/api/v1"

# ─── GDACS Disaster Alerts ──────────────────────────────────────────
GDACS_RSS_URL = "https://www.gdacs.org/XML/rss.xml"
GDACS_UPDATE_INTERVAL = 30  # 30 seconds

STOPWORDS = {
    "a", "an", "the", "is", "it", "in", "on", "at", "to", "for",
    "of", "and", "or", "but", "with", "this", "that", "was", "are",
    "be", "as", "by", "from", "has", "have", "had", "he", "she",
    "they", "we", "you", "i", "its", "his", "her", "their", "our",
    "not", "no", "so", "if", "up", "do", "did", "said", "says",
    "will", "would", "could", "should", "may", "been", "after",
    "before", "more", "also", "about", "than", "then", "there",
    "which", "who", "when", "how", "what", "where", "into", "over",
    "new", "one", "two", "just", "can", "all", "some", "any"
}

EVENT_SPIKE_THRESHOLD = 3
EVENT_WINDOW_SECONDS = 60
TREND_TOP_N = 20
