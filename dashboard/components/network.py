"""
Network Graph Component
Visualizes relationships between events, entities, and keywords
"""

import networkx as nx
import plotly.graph_objects as go
import json
from collections import Counter


def build_event_network(events, max_nodes=100):
    """
    Build network graph from events showing relationships between
    events, entities (locations), and keywords

    Args:
        events: List of event documents from MongoDB
        max_nodes: Maximum number of events to include (for performance)

    Returns:
        NetworkX Graph object
    """
    G = nx.Graph()

    # Limit events for performance
    recent_events = events[:max_nodes] if len(events) > max_nodes else events

    # Count entities and keywords across all events
    entity_counter = Counter()
    keyword_counter = Counter()

    for event in recent_events:
        try:
            entities = json.loads(event.get('entities', '{}'))
            for loc in entities.get('locations', []):
                entity_counter[loc] += 1
        except:
            pass

        keywords = event.get('keywords', [])
        for kw in keywords:
            keyword_counter[kw] += 1

    # Get top entities and keywords
    top_entities = [e for e, c in entity_counter.most_common(20)]
    top_keywords = [k for k, c in keyword_counter.most_common(15)]

    # Build graph
    for i, event in enumerate(recent_events):
        event_id = f"event_{i}"
        cluster = event.get('event_cluster', 'unknown')

        # Add event node
        G.add_node(event_id, type='event', cluster=cluster, label=f"Event: {cluster}")

        # Connect to entities
        try:
            entities = json.loads(event.get('entities', '{}'))
            for loc in entities.get('locations', []):
                if loc in top_entities:
                    G.add_node(loc, type='entity', label=loc)
                    G.add_edge(event_id, loc)
        except:
            pass

        # Connect to keywords
        for kw in event.get('keywords', []):
            if kw in top_keywords:
                G.add_node(kw, type='keyword', label=kw)
                G.add_edge(event_id, kw)

    return G


def calculate_network_statistics(G):
    """
    Calculate network statistics

    Args:
        G: NetworkX Graph object

    Returns:
        Dictionary with network statistics
    """
    num_nodes = G.number_of_nodes()
    num_edges = G.number_of_edges()

    # Calculate average connections (average degree)
    if num_nodes > 0:
        degrees = dict(G.degree())
        avg_connections = sum(degrees.values()) / num_nodes
    else:
        avg_connections = 0

    # Calculate network density
    density = nx.density(G) if num_nodes > 1 else 0

    stats = {
        'nodes': num_nodes,
        'edges': num_edges,
        'avg_connections': round(avg_connections, 2),
        'density': round(density, 3)
    }

    return stats


def create_network_plot(events, max_nodes=100):
    """
    Create interactive network visualization using Plotly

    Args:
        events: List of event documents from MongoDB
        max_nodes: Maximum number of events to include

    Returns:
        Tuple of (Plotly figure object, network statistics dict)
    """
    G = build_event_network(events, max_nodes)

    if G.number_of_nodes() == 0:
        # Return empty figure if no data
        fig = go.Figure()
        fig.update_layout(title_text="No data available for network graph")
        return fig, calculate_network_statistics(G)

    # Calculate layout using spring layout
    pos = nx.spring_layout(G, k=1, iterations=50, seed=42)

    # Extract node positions and attributes
    node_x = []
    node_y = []
    node_text = []
    node_colors = []
    node_sizes = []
    hover_texts = []

    for node in G.nodes():
        x, y = pos[node]
        node_x.append(x)
        node_y.append(y)

        node_data = G.nodes[node]
        node_type = node_data.get('type', 'unknown')

        if node_type == 'event':
            cluster = node_data.get('cluster', 'unknown')
            node_text.append(cluster)
            hover_texts.append(f"<b>Event</b><br>Cluster: {cluster}")
            node_colors.append('#818cf8')  # Blue for events
            node_sizes.append(15)

        elif node_type == 'entity':
            label = node_data.get('label', node)
            node_text.append(label[:15])  # Truncate long labels
            hover_texts.append(f"<b>Location</b><br>{label}")
            node_colors.append('#fbbf24')  # Yellow for entities
            node_sizes.append(25)

        else:  # keyword
            label = node_data.get('label', node)
            node_text.append(label[:12])  # Truncate long labels
            hover_texts.append(f"<b>Keyword</b><br>{label}")
            node_colors.append('#34d399')  # Green for keywords
            node_sizes.append(12)

    # Create edges
    edge_x = []
    edge_y = []

    for edge in G.edges():
        x0, y0 = pos[edge[0]]
        x1, y1 = pos[edge[1]]
        edge_x.extend([x0, x1, None])
        edge_y.extend([y0, y1, None])

    # Create edge trace
    edge_trace = go.Scatter(
        x=edge_x, y=edge_y,
        line=dict(width=0.5, color='#94a3b8'),
        hoverinfo='none',
        mode='lines'
    )

    # Create node trace
    node_trace = go.Scatter(
        x=node_x, y=node_y,
        mode='markers+text',
        hoverinfo='text',
        hovertext=hover_texts,
        text=node_text,
        textposition='middle center',
        textfont=dict(size=9, color='white'),
        marker=dict(
            size=node_sizes,
            color=node_colors,
            line=dict(width=1, color='white')
        ),
        showlegend=False
    )

    # Create figure
    fig = go.Figure(data=[edge_trace, node_trace])

    fig.update_layout(
        title='<b>Event Network Graph</b>',
        showlegend=False,
        hovermode='closest',
        margin=dict(b=20, l=20, r=20, t=60),
        height=700,
        annotations=[
            dict(
                text="<b>Nodes:</b> Events (blue), Locations (yellow), Keywords (green)",
                showarrow=False,
                xref="paper", yref="paper",
                x=0.005, y=-0.02,
                xanchor='left', yanchor='bottom',
                font=dict(size=11, color='#64748b')
            )
        ],
        xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
        yaxis=dict(showgrid=False, zeroline=False, showticklabels=False)
    )

    return fig, calculate_network_statistics(G)