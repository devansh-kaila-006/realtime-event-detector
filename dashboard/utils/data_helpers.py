"""
Data Helper Functions
Cache-optimized data loading functions
"""

import streamlit as st
from pymongo import MongoClient
from datetime import datetime, timedelta
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from config.settings import MONGO_URI, MONGO_DB


def get_mongo_collections():
    """Get MongoDB collections"""
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    events_collection = db["processed_events"]
    keywords_collection = db["keywords"]
    return events_collection, keywords_collection


@st.cache_data(ttl=10)
def fetch_events_cached(limit=100, source_filter=None):
    """Fetch events with caching"""

    events_collection, _ = get_mongo_collections()

    query = {}
    if source_filter:
        query['source_type'] = {'$in': source_filter}

    events = list(
        events_collection.find(query)
        .sort('ingested_at', -1)
        .limit(limit)
    )

    return events


@st.cache_data(ttl=30)
def fetch_metrics_cached():
    """Fetch metrics with caching"""

    events_collection, _ = get_mongo_collections()

    total = events_collection.estimated_document_count()
    wiki = events_collection.count_documents({"source_type": "wikipedia"})
    news = events_collection.count_documents({"source_type": "news"})
    gdacs = events_collection.count_documents({"source_type": "gdacs"})
    financial = events_collection.count_documents({"source_type": "financial"})

    return total, wiki, news, gdacs, financial


def fetch_filtered_events(filters, limit=1000):
    """
    Fetch events with applied filters.
    Not cached by default since filters change frequently.
    """
    events_collection, _ = get_mongo_collections()

    query = {}

    # Apply filters to query
    if filters.get('sources'):
        query['source_type'] = {'$in': filters['sources']}

    if filters.get('clusters'):
        query['event_cluster'] = {'$in': filters['clusters']}

    if filters.get('keywords'):
        query['keywords'] = {'$in': filters['keywords']}

    if filters.get('locations'):
        query['entities.locations'] = {'$in': filters['locations']}

    if filters.get('confidence_range') and filters['confidence_range'] != (0.0, 1.0):
        min_conf, max_conf = filters['confidence_range']
        query['confidence_score'] = {'$gte': min_conf, '$lte': max_conf}

    if filters.get('date_range'):
        start_date, end_date = filters['date_range']
        query['ingested_at'] = {'$gte': start_date, '$lte': end_date}

    if filters.get('selected_event_ids'):
        query['_id'] = {'$in': filters['selected_event_ids']}

    events = list(
        events_collection.find(query)
        .sort('ingested_at', -1)
        .limit(limit)
    )

    return events


def get_event_time_series_data(hours=24, source_filter=None, date_range=None):
    """Get time series data for the last N hours (minute-level for live updates)."""
    events_collection, _ = get_mongo_collections()

    match_query = {
        "ingested_at": {
            "$gte": datetime.utcnow() - timedelta(hours=hours)
        }
    }

    if source_filter:
        source_values = set(source_filter)
        # Backward compatibility for legacy wiki source label.
        if "wikipedia" in source_values:
            source_values.add("wiki")
        match_query["source_type"] = {"$in": list(source_values)}

    if date_range:
        start_date, end_date = date_range
        match_query["ingested_at"] = {"$gte": start_date, "$lte": end_date}

    pipeline = [
        {
            "$match": match_query
        },
        {
            "$group": {
                "_id": {
                    "date": {"$dateToString": {"format": "%Y-%m-%d %H:%M", "date": "$ingested_at"}},
                    "source": "$source_type"
                },
                "count": {"$sum": 1}
            }
        },
        {"$sort": {"_id.date": 1}}
    ]

    results = list(events_collection.aggregate(pipeline))
    return results


def get_cluster_distribution():
    """Get event cluster distribution"""
    events_collection, _ = get_mongo_collections()

    pipeline = [
        {"$match": {"event_cluster": {"$exists": True, "$ne": "general"}}},
        {
            "$group": {
                "_id": "$event_cluster",
                "count": {"$sum": 1}
            }
        },
        {"$sort": {"count": -1}},
        {"$limit": 10}
    ]

    results = list(events_collection.aggregate(pipeline))
    return results
