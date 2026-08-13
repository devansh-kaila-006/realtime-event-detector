# Real-Time Event Detector (Big Data Architecture)

A full-stack, real-time data processing and visualization pipeline designed to monitor global events, analyze sentiment, detect anomalies, and track entity correlations dynamically.

## Architecture Overview

This system is built using a modern Big Data stack simulating a highly scalable streaming architecture:

1. **Data Producer (`producers/mock_producer.py`)**: 
   - Simulates high-throughput streaming data from various global sources (News APIs, GDACS, Financial Tickers, Wikipedia Edits).
   - In a production environment, this would be replaced by Apache Kafka producers connected to live firehoses.

2. **Stream Processing Engine (`spark/spark_consumer.py`)**:
   - Built with **PySpark Structured Streaming**.
   - Responsible for real-time NLP enrichment (Entity Extraction, Sentiment Analysis) and Anomaly Detection.
   - Pushes processed, enriched data into the database layer.

3. **Database Layer (MongoDB)**:
   - Uses MongoDB (preferably running as a Replica Set via Docker) to store processed events and meta-alerts.
   - The backend utilizes MongoDB Change Streams to react instantly to database writes.

4. **Backend API (`backend/main.py`)**:
   - A high-performance **FastAPI** Python server.
   - Maintains persistent **WebSocket** connections to frontend clients.
   - Streams live data directly from the MongoDB database to the UI using Change Streams (with a polling fallback for standalone instances).

5. **Frontend Command Center (`frontend/`)**:
   - A highly interactive, dark-mode "Command Center" dashboard built with Vanilla JavaScript and Vite.
   - **Visualizations**: Uses `Chart.js` for real-time velocity/sentiment graphs, `vis.js` for Force-Directed Entity Correlation Networks, and `Leaflet.js` for geographical mapping.
   - Features dynamic glassmorphism aesthetics, live ticker feeds, and expandable fullscreen charts.

## Prerequisites

- **Docker**: Required to run the MongoDB database seamlessly.
- **Python 3.8+**: For the backend, Spark consumer, and mock producers.
- **Node.js & npm**: For the frontend Vite server.
- **Java (JRE/JDK 8 or 11)**: Required by PySpark to run the streaming engine.

## Installation & Setup

1. **Install Python Dependencies**:
   ```bash
   pip install fastapi uvicorn motor pyspark textblob
   ```

2. **Install Frontend Dependencies**:
   ```bash
   cd frontend
   npm install
   cd ..
   ```

3. **Start the Database**:
   Make sure Docker Desktop is running. Start the MongoDB instance (ensure it is configured as a Replica Set if you want to use optimized Change Streams, otherwise the backend will fallback to polling).
   ```bash
   docker-compose up -d
   ```

## Running the Application

The easiest way to launch the entire stack on Windows is using the provided PowerShell script. This will spin up the Backend, Spark Consumer, Data Producer, and Frontend in separate processes.

```powershell
.\start_all.ps1
```

Once all services are running, open your browser and navigate to:
**http://localhost:5173**

## Features

- **Live Event Feed**: Watch global events stream in with associated metadata, sentiment scores, and severity tags.
- **Geospatial Mapping**: Events with coordinates are instantly plotted onto a live interactive Leaflet world map.
- **Entity Correlation Network**: A dynamic, spring-physics-based force-directed graph (Vis.js) that links real-time events to the entities (people, organizations, locations) they mention.
- **Anomaly Detection**: Critical events trigger system alerts, red map markers, and high-priority dashboard notifications.
- **Interactive UI**: Click on any chart's top right corner to expand it into a fullscreen overlay for deep-dive analysis.

## Troubleshooting

- **Backend / WebSocket not connecting**: Ensure MongoDB is actively running via Docker. If the backend fails to connect, it will crash and the WebSocket will close.
- **PySpark Errors**: Ensure Java is installed and your `JAVA_HOME` environment variable is correctly set.
- **No Data Appearing**: Check the terminal running the `mock_producer.py` to ensure events are being generated, and check the `spark_consumer.py` terminal to ensure they are being processed and pushed to the database.
