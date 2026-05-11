"""
Simple Financial Producer - Demo Mode
Generates simulated market data for demonstration when API is limited
"""

import json
import time
import random
from kafka import KafkaProducer
from datetime import datetime
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from config.settings import KAFKA_BOOTSTRAP, KAFKA_FINANCIAL_TOPIC

# Simulated market data
MARKETS = [
    {"symbol": "SPX", "name": "S&P 500", "base_price": 5200},
    {"symbol": "NDX", "name": "NASDAQ", "base_price": 18500},
    {"symbol": "DJIA", "name": "DOW JONES", "base_price": 39000},
    {"symbol": "FTSE", "name": "FTSE 100", "base_price": 8100},
    {"symbol": "N225", "name": "Nikkei 225", "base_price": 34500}
]

ANOMALY_TYPES = [
    "price_spike",
    "volume_spike",
    "volatility_increase",
    "trend_reversal"
]

SEVERITIES = [
    "low", "moderate", "high", "extreme"
]

def create_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda x: json.dumps(x, default=str).encode("utf-8"),
        retries=5,
        acks="all"
    )

def generate_market_event():
    """Generate a simulated market event."""
    market = random.choice(MARKETS)
    base_price = market["base_price"]

    # Generate realistic price movement
    price_change = random.uniform(-3.0, 3.0)
    current_price = base_price * (1 + price_change / 100)

    # Determine if this is an anomaly
    is_anomaly = abs(price_change) > 1.5

    event = {
        "title": f"Market Update: {market['name']}",
        "symbol": market["symbol"],
        "index_name": market["name"],
        "current_price": f"{current_price:.2f}",
        "price_change_percent": f"{price_change:.2f}%",
        "high": f"{current_price * 1.01:.2f}",
        "low": f"{current_price * 0.99:.2f}",
        "volume_spike": is_anomaly,
        "anomaly_type": random.choice(ANOMALY_TYPES) if is_anomaly else "normal",
        "severity": random.choice(SEVERITIES) if is_anomaly else "low",
        "description": f"Market data for {market['name']}: Price at {current_price:.2f}, Change: {price_change:.2f}%",
        "market": "US" if market["symbol"] in ["SPX", "NDX", "DJIA"] else "Global",
        "timestamp": datetime.now().isoformat(),
        "source_type": "financial"
    }

    return event

def main():
    print("[FINANCIAL PRODUCER] Starting financial market data producer (Demo Mode)...")
    print(f"[FINANCIAL] Monitoring {len(MARKETS)} major indices")
    print("[FINANCIAL] Generating simulated market events every 30 seconds")
    print()

    producer = create_producer()
    sent_count = 0

    try:
        while True:
            try:
                # Generate and send market event
                event = generate_market_event()

                producer.send(KAFKA_FINANCIAL_TOPIC, value=event)
                sent_count += 1

                print(f"[FINANCIAL] Sent {sent_count} events | {event['index_name']}: {event['price_change_percent']} | {event['anomaly_type']}")

                producer.flush()

                # Wait before next event
                time.sleep(30)  # 30 seconds between events

            except KeyboardInterrupt:
                break
            except Exception as e:
                print(f"[FINANCIAL] Error: {e}")
                time.sleep(10)

    except KeyboardInterrupt:
        print()
        print(f"[FINANCIAL] Shutting down... Sent {sent_count} total events")

    producer.close()

if __name__ == "__main__":
    main()
