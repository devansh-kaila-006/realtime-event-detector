"""
Real-Time Event Detection Dashboard - Enhanced with Pattern Analytics
Tabbed interface with advanced visualizations and cross-filtering
"""

import streamlit as st
import pandas as pd
from pymongo import MongoClient
from collections import Counter
import plotly.express as px
from streamlit_autorefresh import st_autorefresh
import json
import sys
import os

# Add parent directory to path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.append(project_root)

# Import new components
from dashboard.components import filters
from dashboard.utils import data_helpers
from dashboard.components.icons import ICONS
from dashboard.components import sankey
from dashboard.components import network

# ============================================================
# PAGE CONFIG
# ============================================================

st.set_page_config(
    page_title="Real-Time Event Detection - Pattern Analytics",
    layout="wide",
    initial_sidebar_state="collapsed"
)

def inject_custom_css():
    css_file = os.path.join(os.path.dirname(__file__), "styles", "main.css")
    if os.path.exists(css_file):
        with open(css_file) as f:
            st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)

inject_custom_css()

# ============================================================
# MONGODB CONNECTION
# ============================================================

client = MongoClient("mongodb://localhost:27017/")
db = client["event_detector"]
events_collection = db["processed_events"]
keywords_collection = db["keywords"]

# ============================================================
# STOPWORDS
# ============================================================

STOPWORDS = {
    "with", "that", "this", "from", "have",
    "were", "their", "about", "there", "would",
    "could", "should", "added", "using", "after",
    "before", "into", "while", "where", "which",
    "because", "reason", "guess", "slow",
    "they", "them", "then", "than", "been",
    "also", "said", "just", "some", "more",
    "such", "very", "only", "much", "many",
    "edit", "page", "article", "updated"
}

# ============================================================
# INITIALIZE SESSION STATE
# ============================================================

filters.init_session_state()

# ============================================================
# CONTROL PANEL (Main Area - No Sidebar)
# ============================================================

st.markdown(f"<h1>{ICONS['analytics']} Event Detection Analytics Dashboard</h1>", unsafe_allow_html=True)
st.markdown("---")

# Auto-refresh
count = st_autorefresh(interval=st.session_state.get('refresh_interval', 6) * 1000, limit=None, key="refresh")

# Control Panel - All filters visible in main area
with st.expander("🎛️ Control Panel & Filters", expanded=True):
    # Basic controls
    col1, col2 = st.columns(2)
    with col1:
        refresh_interval = st.slider("Refresh interval (sec)", 3, 30, 6)
        st.session_state['refresh_interval'] = refresh_interval

    with col2:
        event_limit = st.selectbox("Total events to load", [50, 100, 200, 500], index=0)

    st.markdown("---")

    # Quick filter presets
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        if st.button("High Confidence"):
            st.session_state.filters['confidence_range'] = (0.7, 1.0)
            st.rerun()
    with col2:
        if st.button("Last Hour"):
            from datetime import datetime, timedelta
            st.session_state.filters['date_range'] = (
                datetime.utcnow() - timedelta(hours=1),
                datetime.utcnow()
            )
            st.rerun()
    with col3:
        if st.button("Last 24 Hours"):
            from datetime import datetime, timedelta
            st.session_state.filters['date_range'] = (
                datetime.utcnow() - timedelta(hours=24),
                datetime.utcnow()
            )
            st.rerun()
    with col4:
        if st.button("Reset All"):
            filters.reset_filters()
            st.rerun()

    st.markdown("---")

    # Data Sources filter
    sources = st.multiselect(
        "📊 Data Sources",
        ['wikipedia', 'news', 'gdacs', 'financial'],
        default=st.session_state.filters['sources']
    )
    st.session_state.filters['sources'] = sources

    # Cluster filter
    available_clusters = filters.get_available_clusters(events_collection)
    clusters = []
    if available_clusters:
        clusters = st.multiselect(
            "🎯 Event Clusters",
            available_clusters,
            default=st.session_state.filters['clusters']
        )
        st.session_state.filters['clusters'] = clusters
    else:
        st.session_state.filters['clusters'] = []

    # Keywords filter
    available_keywords = filters.get_available_keywords(keywords_collection)
    keywords = []
    if available_keywords:
        keywords = st.multiselect(
            "🔑 Keywords",
            available_keywords,
            default=st.session_state.filters['keywords']
        )
        st.session_state.filters['keywords'] = keywords
    else:
        st.session_state.filters['keywords'] = []

    # Locations filter
    available_locations = filters.get_available_locations(events_collection)
    locations = []
    if available_locations:
        locations = st.multiselect(
            "📍 Locations",
            available_locations,
            default=st.session_state.filters['locations']
        )
        st.session_state.filters['locations'] = locations
    else:
        st.session_state.filters['locations'] = []

    # Confidence range slider
    conf_range = st.slider(
        "📈 Confidence Score Range",
        0.0, 1.0,
        st.session_state.filters['confidence_range']
    )
    st.session_state.filters['confidence_range'] = conf_range

    # Show active filter count
    active_count = sum([
        len(sources) if len(sources) < 4 else 0,
        len(clusters),
        len(keywords),
        len(locations),
        1 if conf_range != (0.0, 1.0) else 0,
        1 if st.session_state.filters['date_range'] else 0
    ])

    if active_count > 0:
        st.success(f"🎯 {active_count} active filters applied")
    else:
        st.info("ℹ️ No active filters - showing all data")

# ============================================================
# FETCH DATA
# ============================================================

# Fetch filtered events
filtered_events = data_helpers.fetch_filtered_events(st.session_state.filters, event_limit)

# Fetch metrics
total_events, wiki_count, news_count, gdacs_count, financial_count = data_helpers.fetch_metrics_cached()

# ============================================================
# TOP METRICS (Global across all tabs)
# ============================================================

# System Overview header
st.markdown(f"<h2>{ICONS['dashboard']} System Overview</h2>", unsafe_allow_html=True)

metric1, metric2, metric3, metric4, metric5 = st.columns(5)

with metric1:
    st.metric("Total Events", total_events)

with metric2:
    st.metric("Wikipedia", wiki_count)

with metric3:
    st.metric("News", news_count)

with metric4:
    st.metric("Disasters", gdacs_count)

with metric5:
    st.metric("Financial", financial_count)

# Show active filter count and details
active_filters = sum([
    len(st.session_state.filters['sources']) if len(st.session_state.filters['sources']) < 4 else 0,
    len(st.session_state.filters['clusters']),
    len(st.session_state.filters['keywords']),
    len(st.session_state.filters['locations']),
    1 if st.session_state.filters['confidence_range'] != (0.0, 1.0) else 0,
    1 if st.session_state.filters['date_range'] else 0
])

# Display filter status
if active_filters > 0:
    st.markdown(f"<div class='stAlert st-bc'>{ICONS['target']} Showing {len(filtered_events)} events with {active_filters} active filter(s)</div>", unsafe_allow_html=True)

    # Show active filter details
    with st.expander("🔍 Active Filters (click to change in sidebar)", expanded=False):
        if len(st.session_state.filters['sources']) < 4:
            st.write(f"**Sources:** {', '.join(st.session_state.filters['sources'])}")
        if st.session_state.filters['clusters']:
            st.write(f"**Clusters:** {', '.join(st.session_state.filters['clusters'])}")
        if st.session_state.filters['keywords']:
            st.write(f"**Keywords:** {', '.join(st.session_state.filters['keywords'])}")
        if st.session_state.filters['locations']:
            st.write(f"**Locations:** {', '.join(st.session_state.filters['locations'])}")
        if st.session_state.filters['confidence_range'] != (0.0, 1.0):
            st.write(f"**Confidence:** {st.session_state.filters['confidence_range'][0]:.2f} - {st.session_state.filters['confidence_range'][1]:.2f}")
        if st.session_state.filters['date_range']:
            st.write(f"**Date Range:** {st.session_state.filters['date_range'][0]} to {st.session_state.filters['date_range'][1]}")
else:
    st.markdown(f"<div class='stAlert st-bc'>{ICONS['info']} Showing {len(filtered_events)} events (all data)</div>", unsafe_allow_html=True)

st.markdown("---")

# ============================================================
# TABBED INTERFACE
# ============================================================

tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "Overview & Patterns",
    "Network Analysis",
    "Geographic Intelligence",
    "Event Explorer",
    "Real-Time Feed"
])

# ============================================================
# TAB 1: OVERVIEW & PATTERNS
# ============================================================

with tab1:
    st.markdown(f"<h3>{ICONS['analytics']} Pattern Analysis Overview</h3>", unsafe_allow_html=True)

    if not filtered_events:
        st.warning("No events match the current filters. Try adjusting your filters.")
    else:
        # Time series chart
        st.markdown(f"<h4>{ICONS['chart']} Event Trends (24 Hours)</h4>", unsafe_allow_html=True)

        time_series_data = data_helpers.get_event_time_series_data(hours=24)

        if time_series_data:
            df_time = pd.DataFrame(time_series_data)
            df_time['date'] = df_time['_id'].apply(lambda x: x['date'])
            df_time['source'] = df_time['_id'].apply(lambda x: x['source'])
            df_time['count'] = df_time['count']

            fig_time = px.line(
                df_time,
                x='date',
                y='count',
                color='source',
                title='Events Over Time by Source',
                labels={'date': 'Time', 'count': 'Number of Events', 'source': 'Source'}
            )
            fig_time.update_layout(
                paper_bgcolor='rgba(0,0,0,0)',
                plot_bgcolor='rgba(0,0,0,0)',
                font_color='#e2e8f0',
                margin=dict(l=20, r=20, t=40, b=20)
            )
            st.plotly_chart(fig_time, use_container_width=True)
        else:
            st.info("No time series data available for the last 24 hours")

        # Event cluster distribution
        st.markdown(f"<h4>{ICONS['target']} Top Event Clusters</h4>", unsafe_allow_html=True)

        cluster_data = data_helpers.get_cluster_distribution()

        if cluster_data:
            df_clusters = pd.DataFrame(cluster_data)
            fig_clusters = px.bar(
                df_clusters,
                x='count',
                y='_id',
                orientation='h',
                title='Top Event Clusters',
                labels={'count': 'Number of Events', '_id': 'Event Cluster'},
                color_discrete_sequence=['#818cf8']
            )
            fig_clusters.update_layout(
                yaxis={'categoryorder': 'total ascending'},
                paper_bgcolor='rgba(0,0,0,0)',
                plot_bgcolor='rgba(0,0,0,0)',
                font_color='#e2e8f0',
                margin=dict(l=20, r=20, t=40, b=20)
            )
            st.plotly_chart(fig_clusters, use_container_width=True)
        else:
            st.info("No cluster data available")

        # Sankey diagram for event flow analysis
        st.markdown(f"<h4>{ICONS['network']} Event Flow Analysis</h4>", unsafe_allow_html=True)

        try:
            fig_sankey = sankey.create_sankey_diagram(events_collection)
            st.plotly_chart(fig_sankey, use_container_width=True)
        except Exception as e:
            st.error(f"Error loading Sankey diagram: {e}")
            st.info("Sankey diagram requires events with source and cluster data")

# ============================================================
# TAB 2: NETWORK ANALYSIS
# ============================================================

with tab2:
    st.markdown(f"<h3>{ICONS['network']} Event Network Analysis</h3>", unsafe_allow_html=True)
    st.caption("Explore relationships between events, entities, and keywords")

    # Network controls
    col1, col2, col3 = st.columns(3)
    with col1:
        max_nodes = st.slider("Max Events to Display", 50, 500, 100)
    with col2:
        show_entities = st.checkbox("Show Entities", True)
    with col3:
        show_keywords = st.checkbox("Show Keywords", True)

    # Network graph visualization
    try:
        fig_network, net_stats = network.create_network_plot(filtered_events, max_nodes)

        if fig_network and net_stats['nodes'] > 0:
            st.plotly_chart(fig_network, use_container_width=True)

            # Network statistics
            st.markdown(f"<h4>{ICONS['analytics']} Network Statistics</h4>", unsafe_allow_html=True)
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Nodes", net_stats['nodes'])
            with col2:
                st.metric("Edges", net_stats['edges'])
            with col3:
                st.metric("Avg Connections", net_stats['avg_connections'])
            with col4:
                st.metric("Network Density", net_stats['density'])
        else:
            st.warning("No network data available. Need events with keywords and locations.")
    except Exception as e:
        st.error(f"Error loading network graph: {e}")
        st.info("Network graph requires events with keywords and location data")

# ============================================================
# TAB 3: GEOGRAPHIC INTELLIGENCE
# ============================================================

with tab3:
    st.markdown(f"<h3>{ICONS['map']} Geographic Event Intelligence</h3>", unsafe_allow_html=True)

    # Get events with coordinates
    geo_events = [e for e in filtered_events if e.get('latitude') and e.get('longitude')]

    if geo_events:
        st.markdown(f"<div class='stAlert st-bc'>{ICONS['pin']} Showing {len(geo_events)} events with location data</div>", unsafe_allow_html=True)

        # Create map data
        map_data = []
        for event in geo_events:
            map_data.append({
                'lat': float(event['latitude']),
                'lon': float(event['longitude']),
                'title': event.get('title', 'Unknown')[:30],
                'source': event.get('source_type', 'unknown'),
                'cluster': event.get('event_cluster', 'general')
            })

        df_map = pd.DataFrame(map_data)

        # Color by source
        color_map = {
            'wikipedia': 'blue',
            'news': 'green',
            'gdacs': 'red',
            'financial': 'orange'
        }

        st.map(df_map, latitude='lat', longitude='lon', size='lat', color='source')

        # Geographic statistics
        st.markdown(f"<h4>{ICONS['map']} Geographic Statistics</h4>", unsafe_allow_html=True)

        # Count events by location
        location_counts = Counter()
        for event in geo_events:
            try:
                entities = json.loads(event.get('entities', '{}'))
                for loc in entities.get('locations', [])[:3]:
                    location_counts[loc] += 1
            except:
                pass

        if location_counts:
            st.subheader("Top Locations")
            top_locations = location_counts.most_common(10)
            for loc, count in top_locations:
                st.write(f"**{loc}**: {count} events")
    else:
        st.warning("No events with geographic coordinates match your current filters.")
        st.markdown(f"<div class='stAlert st-bc'>{ICONS['info']} Tip: GDACS events typically have the most location data</div>", unsafe_allow_html=True)

# ============================================================
# TAB 4: EVENT EXPLORER
# ============================================================

with tab4:
    st.markdown(f"<h3>{ICONS['search']} Advanced Event Explorer</h3>", unsafe_allow_html=True)

    if not filtered_events:
        st.warning("No events match the current filters.")
    else:
        st.markdown(f"<h4>{ICONS['table']} Event Table ({len(filtered_events)} events)</h4>", unsafe_allow_html=True)

        # Create DataFrame for display
        table_data = []
        for event in filtered_events:
            table_data.append({
                'Title': event.get('title', 'N/A')[:50],
                'Source': event.get('source_type', 'unknown').title(),
                'Cluster': event.get('event_cluster', 'general').title(),
                'Confidence': f"{event.get('confidence_score', 0):.2f}",
                'Time': event.get('ingested_at', 'N/A').strftime('%Y-%m-%d %H:%M') if event.get('ingested_at') else 'N/A'
            })

        df_table = pd.DataFrame(table_data)

        # Display table
        st.dataframe(df_table, use_container_width=True, height=400)

        # Detailed event view
        st.markdown("---")
        st.markdown(f"<h4>{ICONS['target']} Detailed Event View</h4>", unsafe_allow_html=True)

        # Show first few events in detail
        for i, event in enumerate(filtered_events[:5]):
            # Create clickable title with source link
            title = event.get('title', 'Unknown Event')[:60]
            url = event.get('url', event.get('link', ''))

            if url:
                display_title = f"🔗 [{title}...]({url})"
            else:
                display_title = f"{title}..."

            with st.expander(display_title):
                col1, col2 = st.columns([2, 1])

                with col1:
                    st.markdown(f"**Source:** {event.get('source_type', 'unknown').title()}")
                    st.markdown(f"**Published:** {event.get('timestamp', event.get('ingested_at', 'N/A'))}")
                    st.markdown(f"**Cluster:** {event.get('event_cluster', 'general').title()}")
                    st.markdown(f"**Confidence:** {event.get('confidence_score', 0):.2f}")

                    # Show source link if available
                    if url:
                        st.markdown(f"**🔗 Source Link:** [{url}]({url})")

                    # Entities
                    try:
                        entities_data = json.loads(event.get('entities', '{}'))
                        if entities_data.get('locations'):
                            st.markdown(f"**{ICONS['location']} Locations:**", unsafe_allow_html=True)
                            for loc in entities_data['locations'][:5]:
                                st.markdown(f"  - {loc}")
                        if entities_data.get('people'):
                            st.markdown(f"**{ICONS['people']} People:**", unsafe_allow_html=True)
                            for person in entities_data['people'][:3]:
                                st.markdown(f"  - {person}")
                    except:
                        pass

                with col2:
                    # Keywords
                    try:
                        keywords_data = json.loads(event.get('keywords', '[]'))
                        if keywords_data:
                            st.markdown(f"**{ICONS['keyword']} Keywords:**", unsafe_allow_html=True)
                            for kw in keywords_data[:10]:
                                st.markdown(f"  - {kw}")
                    except:
                        pass

                    # Word count
                    st.markdown(f"**Words:** {event.get('word_count', 0)}")

                # Content preview
                st.markdown("---")
                st.markdown(f"**Content Preview:**")
                clean_text = event.get('clean_text', event.get('description', ''))
                if clean_text:
                    st.markdown(clean_text[:300] + "..." if len(clean_text) > 300 else clean_text)

        if len(filtered_events) > 5:
            st.markdown(f"<div class='stAlert st-bc'>{ICONS['info']} Showing first 5 of {len(filtered_events)} events. Adjust filters to see more.</div>", unsafe_allow_html=True)

# ============================================================
# TAB 5: REAL-TIME FEED
# ============================================================

with tab5:
    st.markdown(f"<h3>{ICONS['feed']} Real-Time Event Feed</h3>", unsafe_allow_html=True)
    st.caption("Live event monitoring and alerts")

    # Check alerts
    def check_alerts(events_list):
        """Check for events that meet alert criteria"""
        alerts = []

        # High confidence events
        high_confidence = [e for e in events_list if e.get('confidence_score', 0) > 0.8]
        if high_confidence:
            alerts.append({
                'type': 'high_confidence',
                'severity': 'info',
                'count': len(high_confidence),
                'message': f"{len(high_confidence)} high confidence event(s) detected",
                'icon': 'target'
            })

        # Disaster events
        disasters = [e for e in events_list if e.get('source_type') == 'gdacs']
        if disasters:
            high_severity = [d for d in disasters if d.get('severity') in ['high', 'extreme']]
            if high_severity:
                alerts.append({
                    'type': 'disaster',
                    'severity': 'warning',
                    'count': len(high_severity),
                    'message': f"{len(high_severity)} severe disaster alert(s)",
                    'icon': 'alert'
                })

        # Financial anomalies
        financial_events = [e for e in events_list if e.get('source_type') == 'financial']
        if financial_events:
            severe_financial = [f for f in financial_events if f.get('severity') == 'high']
            if severe_financial:
                alerts.append({
                    'type': 'financial',
                    'severity': 'warning',
                    'count': len(severe_financial),
                    'message': f"{len(severe_financial)} significant financial event(s)",
                    'icon': 'dollar'
                })

        return alerts

    # Display alerts
    alerts = check_alerts(filtered_events)
    if alerts:
        for alert in alerts:
            alert_class = 'st-bb' if alert['severity'] == 'warning' else 'st-bc'
            icon_svg = ICONS.get(alert.get('icon', 'info'))
            st.markdown(f"<div class='stAlert {alert_class}'>{icon_svg} {alert['message']}</div>", unsafe_allow_html=True)

    # Live event feed
    st.markdown("---")
    st.markdown(f"<h4>{ICONS['feed']} Live Event Feed</h4>", unsafe_allow_html=True)

    # Pagination
    page_size = 20
    total_pages = (len(filtered_events) + page_size - 1) // page_size

    if 'feed_page' not in st.session_state:
        st.session_state.feed_page = 1

    col1, col2, col3 = st.columns([1, 2, 1])
    with col1:
        if st.button("Previous") and st.session_state.feed_page > 1:
            st.session_state.feed_page -= 1
            st.rerun()
    with col3:
        if st.button("Next") and st.session_state.feed_page < total_pages:
            st.session_state.feed_page += 1
            st.rerun()
    with col2:
        st.write(f"Page {st.session_state.feed_page} of {total_pages}")

    # Display events for current page
    start_idx = (st.session_state.feed_page - 1) * page_size
    end_idx = start_idx + page_size
    page_events = filtered_events[start_idx:end_idx]

    for event in page_events:
        # Create clickable title with source link
        title = event.get('title', 'Unknown Event')
        url = event.get('url', event.get('link', ''))

        if url:
            # Make title clickable if URL is available
            display_title = f"🔗 [{title}]({url})"
        else:
            display_title = title

        with st.expander(display_title, expanded=False):
            col1, col2 = st.columns([2, 1])

            with col1:
                st.markdown(f"**Source:** {event.get('source_type', 'unknown').title()}")
                st.markdown(f"**Time:** {event.get('ingested_at', 'N/A')}")

                # Show source link if available
                if url:
                    st.markdown(f"**🔗 Source:** [{url}]({url})")

                try:
                    keywords = json.loads(event.get('keywords', '[]'))
                    if keywords:
                        st.markdown(f"**Keywords:** {', '.join(keywords[:5])}")
                except:
                    pass

            with col2:
                st.markdown(f"**Cluster:** {event.get('event_cluster', 'general').title()}")
                st.markdown(f"**Confidence:** {event.get('confidence_score', 0):.2f}")

# ============================================================
# FOOTER
# ============================================================

st.markdown("---")
st.markdown(f"<h3>{ICONS['info']} Tips</h3>", unsafe_allow_html=True)
st.markdown("- Use the **sidebar** to apply cross-filters that work across all tabs")
st.markdown("- **Tab 1 (Overview)** shows pattern analysis and trends")
st.markdown("- **Tab 2 (Network)** will show entity relationships (coming soon)")
st.markdown("- **Tab 3 (Geographic)** shows location-based analysis")
st.markdown("- **Tab 4 (Explorer)** allows deep dive into specific events")
st.markdown("- **Tab 5 (Real-Time)** shows the live feed with alerts")
