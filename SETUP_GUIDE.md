# Real-Time Event Detection System - Complete Setup Guide

## 🚀 Quick Start

### Prerequisites
- Docker & Docker Compose
- Python 3.8+
- 4GB RAM minimum
- 10GB disk space

### Installation Steps

#### 1. Clone and Setup
```bash
cd realtime-event-detector
pip install -r requirements.txt

# Download spaCy model
python -m spacy download en_core_web_sm
```

#### 2. Start Infrastructure
```bash
# Start Kafka, MongoDB, Zookeeper
docker-compose up -d

# Create Kafka topics
docker-compose exec kafka-init bash -c "sleep 20 && kafka-topics --bootstrap-server kafka:29092 --list"
```

#### 3. Setup MongoDB Indexes
```bash
python database/setup_indexes.py
```

#### 4. Start Producers (Separate Terminals)
```bash
# Terminal 1: Wikipedia
python producers/wiki_producer.py

# Terminal 2: News
python producers/news_producer.py

# Terminal 3: GDACS Disasters
python producers/gdacs_producer.py

# Terminal 4: Financial Markets
python producers/financial_producer.py
```

#### 5. Start Spark Consumer
```bash
spark-submit spark/spark_consumer.py
```

#### 6. Start Dashboard
```bash
streamlit run dashboard/app.py
```

#### 7. (Optional) Start WebSocket Server
```bash
python dashboard/websocket_server.py
```

---

## 🔧 Configuration

### API Keys Setup

**File**: `config/settings.py`

```python
# News API (already configured)
NEWS_API_KEY = "your-news-api-key"

# Finnhub API (already configured)
FINNHUB_API_KEY = "your-finnhub-api-key"

# GDACS (no key needed - free RSS feed)
GDACS_RSS_URL = "https://www.gdacs.org/XML/rss.xml"
```

### Kafka Topics

**File**: `docker-compose.yml`

Current topics:
- `wiki_stream` - Wikipedia edits
- `news_stream` - News articles
- `gdacs_stream` - Disaster alerts
- `financial_stream` - Market events
- `processed_events` - Enriched events

---

## 📊 Testing the System

### Run Test Suite
```bash
python testing/test_system.py
```

### Manual Testing Checklist

#### 1. **Data Flow Test**
- ✅ Producers sending data to Kafka
- ✅ Spark consumer processing data
- ✅ Data appearing in MongoDB
- ✅ Dashboard displaying events

#### 2. **NLP Features Test**
- ✅ Sentiment analysis working
- ✅ Event classification correct
- ✅ Named entities extracted
- ✅ Keywords generated

#### 3. **Performance Test**
- ✅ Dashboard refresh smooth (no blocking)
- ✅ Queries respond < 1 second
- ✅ Pagination working
- ✅ Search functionality fast

#### 4. **Advanced Features Test**
- ✅ Geographic map shows location data
- ✅ Alerts trigger on thresholds
- ✅ Advanced search works
- ✅ Data export functional

---

## 🎯 Dashboard Features

### **Real-Time Monitoring**
- Live event feed with pagination
- Auto-refresh every 6 seconds (configurable)
- Source distribution charts
- Trending keywords analysis

### **Advanced Analytics**
- Time-series event tracking
- Sentiment distribution charts
- Event cluster analysis
- Geographic event mapping

### **Search & Filter**
- Full-text search
- Location-based search
- Date range filtering
- Sentiment/cluster/severity filtering

### **Data Export**
- CSV/JSON export
- Configurable event limits
- Real-time data access

---

## 🚨 Alert System

The dashboard automatically alerts on:

- **High Confidence Events** (> 0.8 confidence)
- **Severe Disasters** (high/extreme severity)
- **Negative Sentiment Spikes** (> 0.6 confidence)
- **Financial Anomalies** (high severity)
- **Event Cluster Spikes** (3+ similar events)

---

## 🗺️ Geographic Features

### Supported Location Data
- **GDACS**: Automatic coordinate extraction
- **Other sources**: NER-based location extraction

### Map Features
- Real-time event plotting
- Color-coded by source type
- Size by confidence score
- Hover information

---

## ⚡ Performance Optimization

### MongoDB Indexes
```bash
# Rebuild if needed
python database/setup_indexes.py
```

### Query Optimization
- Use pagination for large datasets
- Leverage source filters
- Utilize date range limits
- Cache frequently accessed data

### Dashboard Performance
- Reduce event limit for faster loading
- Increase refresh interval for less CPU usage
- Use specific filters to reduce data volume

---

## 🔍 Troubleshooting

### **No Events Appearing**

1. **Check Kafka Topics**
```bash
docker-compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list
```

2. **Check Producer Logs**
- Look for connection errors
- Verify API keys are valid
- Check rate limiting

3. **Check Spark Consumer**
- Verify Kafka connection
- Check for processing errors
- Monitor MongoDB writes

### **Dashboard Issues**

1. **Blocking Refresh**
- Ensure `streamlit-autorefresh` is installed
- Check for JavaScript errors
- Clear browser cache

2. **No Geographic Data**
- Wait for GDACS events (disaster-based)
- Verify coordinate extraction working
- Check map rendering in browser

### **Performance Issues**

1. **Slow Queries**
```python
# Check MongoDB indexes
use event_detector
db.processed_events.getIndexes()
```

2. **High Memory Usage**
- Reduce event limits
- Increase pagination
- Clear old checkpoint data

---

## 📈 Advanced Features Usage

### **Temporal Correlation**
```python
# In nlp_pipeline.py
correlations = temporal_correlation(events_list, window_minutes=60)
```

### **Geospatial Clustering**
```python
# Cluster events by location
clusters = geospatial_clustering(events_list, eps_km=100.0)
```

### **Dynamic Classification**
```python
# Adaptive event classification
cluster, confidence = dynamic_event_classification(text)
```

---

## 🔄 System Updates

### **Adding New Data Sources**

1. Create producer in `producers/`
2. Add Kafka topic to `docker-compose.yml`
3. Add schema to `spark/spark_consumer.py`
4. Update dashboard filters

### **Modifying NLP Pipeline**

1. Edit `spark/nlp_pipeline.py`
2. Restart Spark consumer
3. Clear MongoDB if needed
4. Verify with test suite

---

## 📊 Monitoring & Maintenance

### **Daily Checks**
- ✅ Monitor producer error rates
- ✅ Check dashboard responsiveness
- ✅ Verify data freshness
- ✅ Review alert frequency

### **Weekly Maintenance**
- 🔄 Clear old checkpoint data
- 📊 Analyze performance metrics
- 🗑️ Remove old events if needed
- 📈 Review system capacity

### **Monthly Optimization**
- 🔍 Review and optimize indexes
- 📊 Analyze query performance
- 💾 Clean up MongoDB storage
- 🔄 Update dependencies

---

## 🚀 Production Deployment

### **Scaling Considerations**

**Kafka**:
- Increase partition count for high volume
- Add multiple brokers for redundancy
- Configure replication factor

**Spark**:
- Increase worker count for parallelism
- Scale memory for large datasets
- Optimize batch processing intervals

**MongoDB**:
- Enable sharding for large collections
- Configure replica sets for redundancy
- Implement backup strategy

**Dashboard**:
- Use multiple instances with load balancer
- Implement Redis caching layer
- Configure auto-scaling

---

## 📚 API Reference

### **WebSocket Server**
- **URL**: `ws://localhost:8765`
- **Messages**: Event streams, stats, alerts
- **Reconnect**: Automatic with backoff

### **Dashboard API**
- **Export**: CSV/JSON download
- **Search**: Advanced MongoDB queries
- **Filters**: Real-time data filtering

---

## 🆘 Support & Debugging

### **Log Locations**
- **Producers**: Console output
- **Spark**: `logs/` directory
- **Dashboard**: Browser console
- **WebSocket**: Server console

### **Debug Mode**
```python
# Enable in config/settings.py
DEBUG = True
```

### **Performance Profiling**
```python
# MongoDB slow queries
db.setProfilingLevel(1, {slowms: 100})

# Spark UI
# Visit http://localhost:4040
```

---

## ✅ System Requirements Validation

### **Minimum Requirements**
- CPU: 2 cores
- RAM: 4GB
- Storage: 10GB
- Network: Stable connection

### **Recommended Requirements**
- CPU: 4+ cores
- RAM: 8GB+
- Storage: 20GB+
- Network: High-speed connection

---

## 🎓 Learning Resources

### **Technologies Used**
- **Apache Kafka**: Distributed streaming
- **Apache Spark**: Big data processing
- **MongoDB**: NoSQL database
- **spaCy**: NLP processing
- **Streamlit**: Dashboard framework

### **Advanced Concepts**
- Event-driven architecture
- Real-time data processing
- Natural language processing
- Geographic information systems
- Time-series analysis

---

## 📝 Changelog

### **Version 2.0 - Enhanced Edition**
- ✅ Added GDACS disaster monitoring
- ✅ Added financial market data
- ✅ Integrated advanced NLP pipeline
- ✅ Implemented sentiment analysis
- ✅ Added geographic visualization
- ✅ Created real-time WebSocket server
- ✅ Built advanced search system
- ✅ Implemented live alerts
- ✅ Added performance optimization
- ✅ Created comprehensive test suite

### **Version 1.0 - Original**
- Wikipedia monitoring
- News aggregation
- Basic NLP processing
- Simple dashboard

---

**🎉 Congratulations! Your enhanced real-time event detection system is now ready!**

For issues or questions, refer to the troubleshooting section or check the test suite results.