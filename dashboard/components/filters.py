"""
Cross-Filter State Management
Manages filter state across all dashboard visualizations
"""

import streamlit as st
from datetime import datetime, timedelta
from pymongo import MongoClient
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from config.settings import MONGO_URI, MONGO_DB
from dashboard.components.icons import ICONS


def init_session_state():
    """Initialize session state for filters if not exists"""
    if 'filters' not in st.session_state:
        st.session_state.filters = {
            'sources': ['wikipedia', 'news', 'gdacs', 'financial'],
            'clusters': [],
            'keywords': [],
            'locations': [],
            'date_range': None,
            'confidence_range': (0.0, 1.0),
            'selected_event_ids': []
        }

    if 'active_tab' not in st.session_state:
        st.session_state.active_tab = 'overview'


def reset_filters():
    """Reset all filters to default state"""
    st.session_state.filters = {
        'sources': ['wikipedia', 'news', 'gdacs', 'financial'],
        'clusters': [],
        'keywords': [],
        'locations': [],
        'date_range': None,
        'confidence_range': (0.0, 1.0),
        'selected_event_ids': []
    }


def get_available_clusters(events_collection):
    """Get list of available event clusters from database"""
    try:
        clusters = events_collection.distinct("event_cluster")
        return [c for c in clusters if c and c != 'general']
    except:
        return ['earthquake', 'hurricane', 'flood', 'election', 'war', 'ai', 'economy']


def get_available_keywords(keywords_collection, limit=20):
    """Get top keywords from keywords collection"""
    try:
        pipeline = [
            {"$group": {"_id": "$keyword", "count": {"$sum": "$count"}}},
            {"$sort": {"count": -1}},
            {"$limit": limit}
        ]

        results = list(keywords_collection.aggregate(pipeline))
        return [r['_id'] for r in results]
    except:
        return []


def get_available_locations(events_collection, limit=15):
    """Get top locations from entities"""
    try:
        pipeline = [
            {"$match": {"entities.locations": {"$exists": True, "$ne": []}}},
            {"$unwind": "$entities.locations"},
            {"$group": {"_id": "$entities.locations", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": limit}
        ]

        results = list(events_collection.aggregate(pipeline))
        return [r['_id'] for r in results]
    except:
        return []


def render_filter_panel(events_collection, keywords_collection):
    """Render collapsible filter panel in sidebar"""
    st.sidebar.markdown("---")
    st.sidebar.markdown(f"<h3>{ICONS['search']} Cross-Filter Controls</h3>", unsafe_allow_html=True)

    # Quick filter presets
    with st.sidebar.expander("Quick Filters", expanded=False):
        col1, col2 = st.columns(2)
        with col1:
            if st.button("High Confidence"):
                st.session_state.filters['confidence_range'] = (0.7, 1.0)
                st.rerun()
        with col2:
            if st.button("Last Hour"):
                st.session_state.filters['date_range'] = (
                    datetime.utcnow() - timedelta(hours=1),
                    datetime.utcnow()
                )
                st.rerun()

        col3, col4 = st.columns(2)
        with col3:
            if st.button("Last 24 Hours"):
                st.session_state.filters['date_range'] = (
                    datetime.utcnow() - timedelta(hours=24),
                    datetime.utcnow()
                )
                st.rerun()
        with col4:
            if st.button("Reset All"):
                reset_filters()
                st.rerun()

    # Source filter
    st.sidebar.markdown(f"**{ICONS['data']} Data Sources**", unsafe_allow_html=True)
    sources = st.sidebar.multiselect(
        "Filter by source",
        ['wikipedia', 'news', 'gdacs', 'financial'],
        default=st.session_state.filters['sources']
    )
    st.session_state.filters['sources'] = sources

    # Cluster filter
    available_clusters = get_available_clusters(events_collection)
    st.sidebar.markdown(f"**{ICONS['target']} Event Clusters**", unsafe_allow_html=True)
    clusters = st.sidebar.multiselect(
        "Filter by cluster",
        available_clusters,
        default=st.session_state.filters['clusters']
    )
    st.session_state.filters['clusters'] = clusters

    # Confidence range slider
    st.sidebar.markdown(f"**{ICONS['chart']} Confidence Score**", unsafe_allow_html=True)
    conf_range = st.sidebar.slider(
        "Minimum confidence",
        0.0, 1.0,
        st.session_state.filters['confidence_range']
    )
    st.session_state.filters['confidence_range'] = conf_range

    # Keyword filter
    keywords = st.session_state.filters['keywords']
    available_keywords = get_available_keywords(keywords_collection)
    if available_keywords:
        st.sidebar.markdown(f"**{ICONS['keyword']} Keywords**", unsafe_allow_html=True)
        keywords = st.sidebar.multiselect(
            "Filter by keywords",
            available_keywords,
            default=st.session_state.filters['keywords']
        )
        st.session_state.filters['keywords'] = keywords

    # Location filter
    locations = st.session_state.filters['locations']
    available_locations = get_available_locations(events_collection)
    if available_locations:
        st.sidebar.markdown(f"**{ICONS['location']} Locations**", unsafe_allow_html=True)
        locations = st.sidebar.multiselect(
            "Filter by location",
            available_locations,
            default=st.session_state.filters['locations']
        )
        st.session_state.filters['locations'] = locations

    # Active filters summary
    st.sidebar.markdown("---")
    active_count = sum([
        len(sources) if len(sources) < 4 else 0,
        len(clusters),
        len(keywords),
        len(locations),
        1 if conf_range != (0.0, 1.0) else 0,
        1 if st.session_state.filters['date_range'] else 0
    ])

    if active_count > 0:
        st.sidebar.markdown(f"<div class='stAlert st-bc'>{ICONS['target']} {active_count} active filter(s)</div>", unsafe_allow_html=True)
    else:
        st.sidebar.markdown(f"<div class='stAlert st-bc'>{ICONS['info']} No active filters</div>", unsafe_allow_html=True)


def apply_filters(query, filters):
    """
    Apply all active filters to MongoDB query.

    Args:
        query: MongoDB query dictionary (will be modified in place)
        filters: Filter state from session state

    Returns:
        Modified query with filters applied
    """
    # Source filter
    if filters['sources']:
        query['source_type'] = {'$in': filters['sources']}

    # Cluster filter
    if filters['clusters']:
        query['event_cluster'] = {'$in': filters['clusters']}

    # Keyword filter (events containing any of the selected keywords)
    if filters['keywords']:
        query['keywords'] = {'$in': filters['keywords']}

    # Location filter (events containing any of the selected locations)
    if filters['locations']:
        query['entities.locations'] = {'$in': filters['locations']}

    # Confidence score filter
    if filters['confidence_range'] != (0.0, 1.0):
        min_conf, max_conf = filters['confidence_range']
        query['confidence_score'] = {
            '$gte': min_conf,
            '$lte': max_conf
        }

    # Date range filter
    if filters['date_range']:
        start_date, end_date = filters['date_range']
        query['ingested_at'] = {
            '$gte': start_date,
            '$lte': end_date
        }

    # Selected events filter (from drill-down)
    if filters['selected_event_ids']:
        query['_id'] = {'$in': filters['selected_event_ids']}

    return query
