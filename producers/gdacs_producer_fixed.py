"""
GDACS Producer - Fixed for Python 3.13
Uses requests instead of feedparser to avoid cgi module issues
"""

import json
import time
import requests
import xml.etree.ElementTree as ET
from kafka import KafkaProducer
from kafka.errors import KafkaError
from datetime import datetime
import sys
import os
import re

sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from config.settings import KAFKA_BOOTSTRAP, KAFKA_GDACS_TOPIC

# GDACS RSS URL
GDACS_RSS_URL = "https://www.gdacs.org/XML/rss.xml"
GDACS_UPDATE_INTERVAL = 300  # 5 minutes

def create_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda x: json.dumps(x, default=str).encode("utf-8"),
        retries=5,
        acks="all"
    )

def parse_disaster_info(title):
    """Parse disaster type and severity from title."""
    disaster_type = "unknown"
    severity = "unknown"
    alert_level = "green"

    title_lower = title.lower()

    if 'earthquake' in title_lower:
        disaster_type = "earthquake"
        # Extract magnitude
        mag_match = re.search(r'(\d+\.?\d*)\s*m', title_lower)
        if mag_match:
            try:
                magnitude = float(mag_match.group(1))
                if magnitude >= 7.0:
                    severity = "extreme"
                    alert_level = "red"
                elif magnitude >= 6.0:
                    severity = "high"
                    alert_level = "orange"
                elif magnitude >= 5.0:
                    severity = "moderate"
                    alert_level = "yellow"
                else:
                    severity = "low"
                    alert_level = "green"
            except ValueError:
                severity = "moderate"
                alert_level = "yellow"

    elif 'flood' in title_lower or 'flash flood' in title_lower:
        disaster_type = "flood"
        severity = "high"
        alert_level = "orange"

    elif any(x in title_lower for x in ['cyclone', 'hurricane', 'typhoon']):
        disaster_type = "tropical_cyclone"
        severity = "high"
        alert_level = "orange"

    elif 'volcano' in title_lower:
        disaster_type = "volcanic_eruption"
        severity = "high"
        alert_level = "orange"

    elif 'tsunami' in title_lower:
        disaster_type = "tsunami"
        severity = "extreme"
        alert_level = "red"

    return disaster_type, severity, alert_level

def extract_coordinates(description):
    """Extract lat/lon from description if available."""
    lat_match = re.search(r'lat.\s*(-?\d+\.?\d*)', description, re.IGNORECASE)
    lon_match = re.search(r'long.\s*(-?\d+\.?\d*)', description, re.IGNORECASE)

    latitude = lat_match.group(1) if lat_match else None
    longitude = lon_match.group(1) if lon_match else None

    return latitude, longitude

def fetch_gdacs_events():
    """Fetch disaster events from GDACS RSS feed."""
    try:
        response = requests.get(GDACS_RSS_URL, timeout=30)
        response.raise_for_status()

        # Parse XML
        root = ET.fromstring(response.content)

        events = []
        for item in root.findall('.//item'):
            event = {}

            # Extract basic fields
            title = item.find('title')
            event['title'] = title.text if title is not None else 'Unknown Disaster'

            description = item.find('description')
            event['description'] = description.text if description is not None else ''

            link = item.find('link')
            event['source_url'] = link.text if link is not None else ''

            pub_date = item.find('pubDate')
            event['timestamp'] = pub_date.text if pub_date is not None else datetime.utcnow().isoformat()

            # Parse disaster information
            disaster_type, severity, alert_level = parse_disaster_info(event['title'])
            event['disaster_type'] = disaster_type
            event['severity'] = severity
            event['alert_level'] = alert_level

            # Extract coordinates
            lat, lon = extract_coordinates(event['description'])
            event['latitude'] = lat
            event['longitude'] = lon

            event['source_type'] = 'gdacs'

            events.append(event)

        return events

    except Exception as e:
        print(f"[GDACS] Error fetching events: {e}")
        return []

def main():
    print("[GDACS PRODUCER] Starting GDACS disaster alert producer...")
    print(f"[GDACS] RSS URL: {GDACS_RSS_URL}")
    print(f"[GDACS] Update interval: {GDACS_UPDATE_INTERVAL}s")
    print()

    producer = create_producer()
    sent_count = 0

    try:
        while True:
            try:
                print(f"[GDACS] Fetching disaster alerts...")
                events = fetch_gdacs_events()

                if events:
                    print(f"[GDACS] Found {len(events)} disaster alerts")

                    for event in events:
                        producer.send(KAFKA_GDACS_TOPIC, value=event)
                        sent_count += 1

                        if sent_count % 5 == 0:
                            print(f"[GDACS] Sent {sent_count} total alerts | Latest: {event['title'][:40]}...")

                    producer.flush()
                    print(f"[GDACS] Successfully sent {len(events)} alerts to Kafka")
                else:
                    print("[GDACS] No events found in RSS feed")

                print(f"[GDACS] Waiting {GDACS_UPDATE_INTERVAL}s before next update...")
                print()
                time.sleep(GDACS_UPDATE_INTERVAL)

            except KeyboardInterrupt:
                break
            except Exception as e:
                print(f"[GDACS] Error in main loop: {e}")
                time.sleep(60)

    except KeyboardInterrupt:
        print()
        print(f"[GDACS] Shutting down... Sent {sent_count} total alerts")

    producer.close()

if __name__ == "__main__":
    main()
