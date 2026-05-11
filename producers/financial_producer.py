"""
Financial Market Data → Kafka producer.
Monitors major stock indices using Finnhub API and detects market anomalies.
"""

import json
import time
import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError
from datetime import datetime, timedelta
from collections import deque
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from config.settings import (
    KAFKA_BOOTSTRAP,
    KAFKA_FINANCIAL_TOPIC,
    FINNHUB_API_KEY,
    FINNHUB_API_BASE
)

# Major indices to monitor
INDICES = {
    "^GSPC": "S&P 500",      # US
    "^IXIC": "NASDAQ",       # US
    "^DJI": "DOW JONES",     # US
    "^FTSE": "FTSE 100",     # UK
    "^N225": "Nikkei 225",   # Japan
    "^HSI": "Hang Seng",     # Hong Kong
}

# Anomaly detection parameters
PRICE_CHANGE_THRESHOLD = 5.0  # 5% price change
VOLUME_SPIKE_MULTIPLIER = 2.0  # 2x normal volume
UPDATE_INTERVAL = 30  # 30 seconds between updates
PRICE_HISTORY_LENGTH = 20  # Keep 20 data points for baseline


def create_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda x: json.dumps(x, default=str).encode("utf-8"),
        retries=5,
        acks="all"
    )


def fetch_index_data(symbol: str) -> dict:
    """Fetch current and historical data for an index using Finnhub API."""
    try:
        # Get quote data
        quote_url = f"{FINNHUB_API_BASE}/quote"
        params = {
            "symbol": symbol,
            "token": FINNHUB_API_KEY
        }

        response = requests.get(quote_url, params=params, timeout=10)
        response.raise_for_status()

        data = response.json()

        # Calculate price change percentage
        current_price = data.get("c", 0)  # Current price
        previous_close = data.get("pc", 0)  # Previous close

        if current_price and previous_close and previous_close > 0:
            price_change_percent = ((current_price - previous_close) / previous_close) * 100
        else:
            price_change_percent = 0.0

        return {
            "symbol": symbol,
            "name": INDICES.get(symbol, symbol),
            "current_price": current_price,
            "previous_close": previous_close,
            "price_change_percent": price_change_percent,
            "high": data.get("h", 0),
            "low": data.get("l", 0),
            "open": data.get("o", 0),
            "timestamp": datetime.utcnow().isoformat()
        }

    except requests.exceptions.RequestException as e:
        print(f"[Financial Producer] Error fetching data for {symbol}: {e}")
        return None


def detect_anomalies(current_data: dict, price_history: dict) -> list:
    """Detect market anomalies based on price and volume data."""
    anomalies = []

    if not current_data:
        return anomalies

    symbol = current_data["symbol"]
    price_change = current_data["price_change_percent"]

    # Check for significant price movement
    if abs(price_change) >= PRICE_CHANGE_THRESHOLD:
        direction = "SURGE" if price_change > 0 else "DROP"
        anomalies.append({
            "type": "price_movement",
            "severity": "high" if abs(price_change) >= 7.0 else "moderate",
            "description": f"{current_data['name']} {direction}: {price_change:.2f}%"
        })

    # Check for volume spikes (if we had volume data - Finnhub basic quote doesn't include it)
    # This would require the premium API or candle data

    # Check if hitting daily high/low
    if current_data["high"] and current_data["current_price"]:
        if current_data["current_price"] >= current_data["high"] * 0.999:
            anomalies.append({
                "type": "daily_high",
                "severity": "low",
                "description": f"{current_data['name']} at daily high: ${current_data['current_price']:.2f}"
            })

    return anomalies


def build_anomaly_message(index_data: dict, anomalies: list) -> dict:
    """Build a structured message for detected anomalies."""
    primary_anomaly = anomalies[0] if anomalies else {
        "type": "market_update",
        "severity": "info",
        "description": f"{index_data['name']} price update"
    }

    return {
        "title": f"{index_data['name']} Market Event",
        "symbol": index_data["symbol"],
        "index_name": index_data["name"],
        "current_price": index_data["current_price"],
        "price_change_percent": index_data["price_change_percent"],
        "anomaly_type": primary_anomaly["type"],
        "severity": primary_anomaly["severity"],
        "description": primary_anomaly["description"],
        "high": index_data["high"],
        "low": index_data["low"],
        "volume_spike": None,  # Not available in basic API
        "market": "global",
        "timestamp": index_data["timestamp"],
        "source_type": "financial"
    }


def main():
    producer = create_producer()

    # Store price history for each index
    price_history = {symbol: deque(maxlen=PRICE_HISTORY_LENGTH) for symbol in INDICES.keys()}

    print(f"\n[Financial Producer] Starting market monitoring...\n")
    print(f"[Financial Producer] Monitoring {len(INDICES)} major indices")
    print(f"[Financial Producer] Update interval: {UPDATE_INTERVAL}s")
    print(f"[Financial Producer] Anomaly threshold: ±{PRICE_CHANGE_THRESHOLD}%\n")

    while True:
        try:
            anomalies_detected = 0

            for symbol in INDICES.keys():
                # Fetch current data
                index_data = fetch_index_data(symbol)

                if not index_data:
                    continue

                # Store in history
                price_history[symbol].append(index_data["current_price"])

                # Detect anomalies
                anomalies = detect_anomalies(index_data, price_history)

                # Always emit at least one market update per symbol each cycle.
                messages = [build_anomaly_message(index_data, [])]
                if anomalies:
                    messages.extend(build_anomaly_message(index_data, [anomaly]) for anomaly in anomalies)

                for message in messages:
                    try:
                        future = producer.send(KAFKA_FINANCIAL_TOPIC, value=message)
                        future.get(timeout=5)

                        if message["anomaly_type"] != "market_update":
                            anomalies_detected += 1

                        print(f"[Financial] {message['anomaly_type']:20s} | "
                              f"{message['severity']:10s} | {message['description'][:70]}")

                    except KafkaError as e:
                        print(f"[Financial Producer] Kafka send error: {e}")
                        continue

            if anomalies_detected == 0:
                print("[Financial Producer] No anomalies detected; market updates were still published.")

            print(f"\n[Financial Producer] Cycle complete. Waiting {UPDATE_INTERVAL}s...\n")

        except Exception as e:
            print(f"[Financial Producer] Error in main loop: {e}")

        # Wait before next update
        time.sleep(UPDATE_INTERVAL)


if __name__ == "__main__":
    main()
