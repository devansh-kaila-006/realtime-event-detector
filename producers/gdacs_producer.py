"""
GDACS (Global Disaster Alert and Coordination System) → Kafka producer.
Polls GDACS RSS feed for disaster alerts and publishes clean JSON messages.
"""

import json
import time
import requests
import xml.etree.ElementTree as ET
from kafka import KafkaProducer
from kafka.errors import KafkaError
from datetime import datetime, timezone
import sys
import os
import re

sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from config.settings import (
    KAFKA_BOOTSTRAP,
    KAFKA_GDACS_TOPIC,
    GDACS_RSS_URL,
    GDACS_UPDATE_INTERVAL
)


def create_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda x: json.dumps(x, default=str).encode("utf-8"),
        retries=5,
        acks="all"
    )


def parse_gdacs_entry(entry) -> dict:
    """Parse GDACS RSS feed entry into structured disaster event data."""

    # Extract basic information
    title = entry.get("title", "")
    description = entry.get("description", "")
    link = entry.get("link", "")

    # Parse disaster type and severity from title
    # GDACS title format: "Earthquake of 6.5M, Depth: 10km, Date:..."
    disaster_type = "unknown"
    severity = "unknown"

    title_lower = title.lower()
    if "earthquake" in title_lower:
        disaster_type = "earthquake"
        magnitude_match = re.search(r"(\d+\.?\d*)\s*m", title_lower)
        if magnitude_match:
            try:
                magnitude = float(magnitude_match.group(1))
                if magnitude >= 7.0:
                    severity = "extreme"
                elif magnitude >= 6.0:
                    severity = "high"
                elif magnitude >= 5.0:
                    severity = "moderate"
                else:
                    severity = "low"
            except ValueError:
                severity = "moderate"

    elif "flood" in title_lower or "flash flood" in title_lower:
        disaster_type = "flood"
        severity = "high"  # Floods are generally severe

    elif "cyclone" in title_lower or "hurricane" in title_lower or "typhoon" in title_lower:
        disaster_type = "tropical_cyclone"
        severity = "high"

    elif "volcano" in title_lower:
        disaster_type = "volcanic_eruption"
        severity = "high"

    elif "tsunami" in title_lower:
        disaster_type = "tsunami"
        severity = "extreme"

    # Extract coordinates from description if available
    latitude = None
    longitude = None

    # GDACS descriptions often contain lat/lon
    if description:
        # Look for coordinate patterns
        coord_pattern = r"(\d+\.?\d*)[°\s]+([NS])[,;\s]+(\d+\.?\d*)[°\s]+([EW])"
        coords = re.search(coord_pattern, description)
        if coords:
            try:
                lat = float(coords.group(1))
                lat_dir = coords.group(2)
                lon = float(coords.group(3))
                lon_dir = coords.group(4)

                latitude = -lat if lat_dir == "S" else lat
                longitude = -lon if lon_dir == "W" else lon
            except (ValueError, IndexError):
                pass

    # Parse timestamp
    timestamp = entry.get("published", datetime.now(timezone.utc).isoformat())

    return {
        "title": title,
        "description": description,
        "disaster_type": disaster_type,
        "severity": severity,
        "latitude": latitude,
        "longitude": longitude,
        "alert_level": severity,  # Map severity to alert level
        "source_url": link,
        "timestamp": timestamp,
        "source_type": "gdacs"
    }


def fetch_gdacs_alerts():
    """Fetch disaster alerts from GDACS RSS feed."""
    try:
        response = requests.get(GDACS_RSS_URL, timeout=30)
        response.raise_for_status()

        root = ET.fromstring(response.content)
        entries = []
        for item in root.findall(".//item"):
            entries.append({
                "title": item.findtext("title", default=""),
                "description": item.findtext("description", default=""),
                "link": item.findtext("link", default=""),
                "published": item.findtext("pubDate", default=datetime.now(timezone.utc).isoformat()),
            })
        return entries
    except Exception as e:
        print(f"[GDACS Producer] Error fetching RSS feed: {e}")
        return []


def main():
    producer = create_producer()

    # Track processed alert IDs to avoid duplicates
    processed_alerts = set()

    print(f"\n[GDACS Producer] Starting disaster alert monitoring...\n")
    print(f"[GDACS Producer] Fetching from: {GDACS_RSS_URL}")
    print(f"[GDACS Producer] Update interval: {GDACS_UPDATE_INTERVAL}s\n")

    while True:
        try:
            entries = fetch_gdacs_alerts()

            if not entries:
                print("[GDACS Producer] No alerts found in RSS feed")
            else:
                print(f"[GDACS Producer] Found {len(entries)} alerts in RSS feed")

                new_alerts_count = 0

                for entry in entries:
                    # Create unique ID from link or title
                    alert_id = entry.get("link", entry.get("title", ""))

                    if alert_id in processed_alerts:
                        continue

                    # Parse the alert
                    alert_data = parse_gdacs_entry(entry)

                    # Send to Kafka
                    try:
                        future = producer.send(KAFKA_GDACS_TOPIC, value=alert_data)
                        future.get(timeout=5)

                        processed_alerts.add(alert_id)
                        new_alerts_count += 1

                        print(f"[GDACS] {alert_data['disaster_type']:20s} | "
                              f"{alert_data['severity']:10s} | {alert_data['title'][:60]}")

                    except KafkaError as e:
                        print(f"[GDACS Producer] Kafka send error: {e}")
                        continue

                print(f"\n[GDACS Producer] Sent {new_alerts_count} new alerts. "
                      f"Waiting {GDACS_UPDATE_INTERVAL}s...\n")

        except Exception as e:
            print(f"[GDACS Producer] Error in main loop: {e}")

        # Wait before next update
        time.sleep(GDACS_UPDATE_INTERVAL)


if __name__ == "__main__":
    main()
