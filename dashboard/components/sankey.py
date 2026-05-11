"""
Sankey Diagram Component
Visualizes event flow: Source → Cluster → Sentiment
"""

import plotly.graph_objects as go


def create_sankey_diagram(events_collection):
    """
    Create Sankey diagram showing Source → Cluster → Sentiment flow

    Args:
        events_collection: MongoDB collection object

    Returns:
        Plotly figure object with Sankey diagram
    """

    # Fetch data from MongoDB
    pipeline = [
        {
            "$project": {
                "source": {
                    "$cond": [
                        {"$eq": ["$source_type", "wiki"]},
                        "wikipedia",
                        "$source_type"
                    ]
                },
                "cluster": {"$ifNull": ["$event_cluster", "general"]},
                "sentiment": {
                    "$cond": [
                        {"$in": ["$sentiment", ["positive", "negative", "neutral"]]},
                        "$sentiment",
                        "neutral"
                    ]
                }
            }
        },
        {"$group": {
            "_id": {
                "source": "$source",
                "cluster": "$cluster",
                "sentiment": "$sentiment"
            },
            "count": {"$sum": 1}
        }}
    ]

    data = list(events_collection.aggregate(pipeline))

    if not data:
        # Return empty figure if no data
        fig = go.Figure()
        fig.update_layout(title_text="No data available for Sankey diagram")
        return fig

    # Define node labels
    sources = ['wikipedia', 'news', 'gdacs', 'financial']
    clusters = list(set([d['_id']['cluster'] for d in data if d['_id']['cluster']]))
    sentiments = ['positive', 'negative', 'neutral']

    # Filter out empty/None clusters
    clusters = [c for c in clusters if c and c != 'general']

    # Create node list
    nodes = sources + clusters + sentiments

    # Create links (aggregated to avoid duplicate edges)
    source_cluster_links = {}
    cluster_sentiment_links = {}

    for d in data:
        source = d['_id']['source']
        cluster = d['_id']['cluster']
        sentiment = d['_id']['sentiment']
        count = d['count']

        # Only add links if all components exist
        if source in sources and cluster in clusters and sentiment in sentiments:
            source_cluster_links[(source, cluster)] = source_cluster_links.get((source, cluster), 0) + count
            cluster_sentiment_links[(cluster, sentiment)] = cluster_sentiment_links.get((cluster, sentiment), 0) + count

    links = []

    for (source, cluster), count in source_cluster_links.items():
        source_idx = sources.index(source)
        cluster_idx = len(sources) + clusters.index(cluster)
        links.append({
            'source': source_idx,
            'target': cluster_idx,
            'value': count,
            'color': get_source_color(source)
        })

    for (cluster, sentiment), count in cluster_sentiment_links.items():
        cluster_idx = len(sources) + clusters.index(cluster)
        sentiment_idx = len(sources) + len(clusters) + sentiments.index(sentiment)
        links.append({
            'source': cluster_idx,
            'target': sentiment_idx,
            'value': count,
            'color': get_sentiment_color(sentiment)
        })

    if not links:
        fig = go.Figure()
        fig.update_layout(title_text="No flow data available for Sankey diagram")
        return fig

    # Create Sankey figure
    fig = go.Figure(data=[go.Sankey(
        node=dict(
            pad=15,
            thickness=20,
            line=dict(color='black', width=0.5),
            label=nodes,
            color=[get_node_color(node, sources, clusters, sentiments) for node in nodes]
        ),
        link=dict(
            source=[l['source'] for l in links],
            target=[l['target'] for l in links],
            value=[l['value'] for l in links],
            color=[l['color'] for l in links]
        )
    )])

    fig.update_layout(
        title_text="<b>Event Flow Analysis</b><br>Source → Cluster → Sentiment",
        font_size=12,
        height=600
    )

    return fig


def get_source_color(source):
    """Get color for source nodes"""
    colors = {
        'wikipedia': '#3B82F6',      # Blue
        'news': '#10B981',           # Green
        'gdacs': '#EF4444',          # Red
        'financial': '#F59E0B'       # Orange
    }
    return colors.get(source, '#6B7280')


def get_sentiment_color(sentiment):
    """Get color for sentiment nodes"""
    colors = {
        'positive': '#10B981',       # Green
        'negative': '#EF4444',       # Red
        'neutral': '#6B7280'         # Gray
    }
    return colors.get(sentiment, '#6B7280')


def get_cluster_color(cluster):
    """Get color for cluster nodes"""
    # Use a consistent color based on cluster name hash
    colors = ['#8B5CF6', '#EC4899', '#F97316', '#14B8A6', '#6366F1',
              '#F59E0B', '#84CC16', '#06B6D4', '#F43F5E', '#A855F7']
    # Simple hash for consistent colors
    idx = hash(cluster) % len(colors)
    return colors[idx]


def get_node_color(node, sources, clusters, sentiments):
    """Get color for a node based on its type"""
    if node in sources:
        return get_source_color(node)
    elif node in clusters:
        return get_cluster_color(node)
    elif node in sentiments:
        return get_sentiment_color(node)
    else:
        return '#6B7280'  # Default gray
