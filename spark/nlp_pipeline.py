"""
NLP Pipeline — Phase 5.
Implements keyword extraction, NER, spike detection, event clustering,
and confidence scoring as pure Python functions (Spark UDFs).

Design: Python UDFs over Spark DataFrames avoids the complexity of
SparkNLP installation while keeping everything in the Spark pipeline.
For production scale, swap these UDFs for SparkNLP annotators.
"""

import re
import math
import json
from collections import Counter, defaultdict
from typing import Optional

# ─────────────────────────────────────────────────────────────────
# spaCy for NER  (pip install spacy && python -m spacy download en_core_web_sm)
# Falls back gracefully if spaCy is not available.
# ─────────────────────────────────────────────────────────────────
try:
    import spacy
    nlp = spacy.load("en_core_web_sm")
    SPACY_AVAILABLE = True
except Exception:
    SPACY_AVAILABLE = False
    print("[NLP] spaCy not available — NER disabled. Run: pip install spacy && python -m spacy download en_core_web_sm")

from config.settings import STOPWORDS, EVENT_SPIKE_THRESHOLD

# ─────────────────────────────────────────────────────────────────
# EVENT CLUSTERS — keyword → canonical event label mapping
# ─────────────────────────────────────────────────────────────────
EVENT_CLUSTERS = {
    "earthquake": ["earthquake", "quake", "tremor", "seismic", "magnitude", "richter", "aftershock"],
    "hurricane":  ["hurricane", "typhoon", "cyclone", "storm", "tropical", "windstorm"],
    "flood":      ["flood", "flooding", "inundation", "deluge", "overflow", "submerged"],
    "fire":       ["fire", "wildfire", "blaze", "inferno", "arson", "burned", "flames"],
    "election":   ["election", "vote", "ballot", "primary", "candidate", "campaign", "polling"],
    "war":        ["war", "conflict", "military", "attack", "invasion", "troops", "airstrike", "ceasefire"],
    "pandemic":   ["pandemic", "outbreak", "virus", "epidemic", "infection", "quarantine", "vaccine"],
    "economy":    ["economy", "inflation", "recession", "gdp", "market", "stocks", "interest rate", "fed"],
    "ai":         ["ai", "artificial intelligence", "machine learning", "llm", "chatgpt", "openai", "neural"],
    "protest":    ["protest", "demonstration", "rally", "riot", "uprising", "march", "strike"],
    "terrorism":  ["terrorism", "terrorist", "bombing", "attack", "explosion", "shooter", "hostage"],
    "climate":    ["climate", "global warming", "carbon", "emissions", "drought", "temperature", "glacier"],
    "space":      ["space", "nasa", "rocket", "satellite", "astronaut", "orbit", "launch", "mars"],
    "technology": ["technology", "tech", "software", "hardware", "apple", "google", "microsoft", "chip"],
    "health":     ["health", "hospital", "drug", "cancer", "surgery", "treatment", "fda", "clinical"],
}

# Reverse lookup: keyword → cluster name
KEYWORD_TO_CLUSTER = {}
for cluster, keywords in EVENT_CLUSTERS.items():
    for kw in keywords:
        KEYWORD_TO_CLUSTER[kw] = cluster


# ─────────────────────────────────────────────────────────────────
# TEXT PREPROCESSING
# ─────────────────────────────────────────────────────────────────

def preprocess(text: Optional[str]) -> list[str]:
    """
    Lowercase, remove punctuation, tokenize, remove stopwords.
    Returns a list of clean tokens.
    """
    if not text:
        return []
    text = text.lower()
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    tokens = text.split()
    tokens = [t for t in tokens if len(t) > 2 and t not in STOPWORDS]
    return tokens


def preprocess_text(text: Optional[str]) -> str:
    """Spark UDF wrapper — returns space-joined token string."""
    return " ".join(preprocess(text))


# ─────────────────────────────────────────────────────────────────
# KEYWORD EXTRACTION  (TF-IDF approximation over a batch)
# ─────────────────────────────────────────────────────────────────

def extract_keywords_tfidf(texts: list[str], top_n: int = 10) -> list[str]:
    """
    Simple TF-IDF over a batch of texts.
    Returns the top_n most distinctive keywords across the batch.
    """
    tf_totals = Counter()
    doc_freq  = Counter()
    n_docs    = len(texts)

    for text in texts:
        tokens = preprocess(text)
        tf_totals.update(tokens)
        doc_freq.update(set(tokens))   # count each word once per doc

    tfidf_scores = {}
    for word, tf in tf_totals.items():
        df = doc_freq[word]
        idf = math.log((n_docs + 1) / (df + 1)) + 1
        tfidf_scores[word] = tf * idf

    return [kw for kw, _ in sorted(tfidf_scores.items(), key=lambda x: -x[1])[:top_n]]


def extract_keywords_single(text: Optional[str], top_n: int = 5) -> str:
    """
    Spark UDF wrapper — extract top keywords from a single text.
    Returns JSON array string for Spark compatibility.
    """
    tokens = preprocess(text)
    freq = Counter(tokens)
    keywords = [w for w, _ in freq.most_common(top_n)]
    return json.dumps(keywords)


# ─────────────────────────────────────────────────────────────────
# NAMED ENTITY RECOGNITION
# ─────────────────────────────────────────────────────────────────

def extract_entities(text: Optional[str]) -> str:
    """
    Spark UDF — extract named entities using spaCy.
    Returns JSON: {"locations": [...], "people": [...], "orgs": [...]}
    Falls back to empty dicts if spaCy is unavailable.
    """
    result = {"locations": [], "people": [], "orgs": [], "countries": []}
    if not text or not SPACY_AVAILABLE:
        return json.dumps(result)

    doc = nlp(text[:1000])   # cap at 1000 chars for speed

    for ent in doc.ents:
        label = ent.label_
        name  = ent.text.strip()
        if label in ("GPE", "LOC"):
            result["locations"].append(name)
            result["countries"].append(name)    # approximate: treat GPE as country candidate
        elif label == "PERSON":
            result["people"].append(name)
        elif label == "ORG":
            result["orgs"].append(name)

    # Deduplicate
    for k in result:
        result[k] = list(set(result[k]))

    return json.dumps(result)


# ─────────────────────────────────────────────────────────────────
# EVENT CLUSTERING
# ─────────────────────────────────────────────────────────────────

def classify_event(text: Optional[str]) -> str:
    """
    Spark UDF — map text to the best matching event cluster.
    Returns the cluster name (e.g. 'earthquake') or 'general'.
    """
    if not text:
        return "general"
    lower = text.lower()

    scores = defaultdict(int)
    for keyword, cluster in KEYWORD_TO_CLUSTER.items():
        if keyword in lower:
            scores[cluster] += 1

    if not scores:
        return "general"
    return max(scores, key=scores.get)


# ─────────────────────────────────────────────────────────────────
# EVENT CONFIDENCE SCORING
# ─────────────────────────────────────────────────────────────────

def compute_confidence(
    keyword_count:   int,
    has_entities:    bool,
    source_count:    int,
    wiki_edit_count: int,
    cluster:         str,
) -> float:
    """
    Composite confidence score between 0.0 and 1.0.

    Formula weights:
    - keyword density    (30%) — more matched keywords → higher confidence
    - entity presence    (20%) — named entities = more specific event
    - source diversity   (25%) — event seen in multiple sources
    - wiki edit activity (25%) — spike in Wikipedia edits = breaking event
    """
    kw_score     = min(keyword_count / 10, 1.0) * 0.30
    entity_score = (0.20 if has_entities else 0.0)
    source_score = min(source_count / 5, 1.0) * 0.25
    wiki_score   = min(wiki_edit_count / EVENT_SPIKE_THRESHOLD, 1.0) * 0.25

    return round(kw_score + entity_score + source_score + wiki_score, 3)


def score_event_udf(keyword_json: str, entity_json: str) -> float:
    """
    Spark UDF wrapper for confidence scoring from parsed JSON strings.
    """
    try:
        keywords = json.loads(keyword_json) if keyword_json else []
        entities = json.loads(entity_json)  if entity_json  else {}
        has_ents = any(len(v) > 0 for v in entities.values())
        return compute_confidence(
            keyword_count=len(keywords),
            has_entities=has_ents,
            source_count=1,
            wiki_edit_count=1,
            cluster="general"
        )
    except Exception:
        return 0.0


# ─────────────────────────────────────────────────────────────────
# SPIKE DETECTION  (used in the Spark foreachBatch handler)
# ─────────────────────────────────────────────────────────────────

# In-memory rolling window for batch-level spike detection.
# In production, persist this in Redis or a Spark accumulator.
_keyword_history: Counter = Counter()
_spike_log: list = []


def detect_spikes(keyword_counts: Counter, threshold: int = EVENT_SPIKE_THRESHOLD) -> list[str]:
    """
    Compare current batch keyword counts to rolling history.
    Returns keywords that have spiked above threshold × rolling mean.
    """
    global _keyword_history

    spikes = []
    for word, count in keyword_counts.items():
        history = _keyword_history.get(word, 0)
        if history > 0 and count >= threshold * history:
            spikes.append(word)
        elif history == 0 and count >= threshold:
            spikes.append(word)   # new keyword appearing for the first time

    # Update history with exponential moving average (α = 0.3)
    for word, count in keyword_counts.items():
        _keyword_history[word] = round(
            0.3 * count + 0.7 * _keyword_history.get(word, 0), 2
        )

    return spikes


# ─────────────────────────────────────────────────────────────────
# TEMPORAL EVENT CORRELATION
# ─────────────────────────────────────────────────────────────────

def temporal_correlation(events: list, window_minutes: int = 60) -> list:
    """
    Group events by time windows and detect correlations.
    Returns groups of related events with correlation scores.

    This function identifies events that occur within the same time window
    and may be related (e.g., earthquake followed by tsunami reports).
    """
    from datetime import datetime, timedelta
    from collections import defaultdict

    if not events:
        return []

    # Parse and sort events by timestamp
    def parse_timestamp(ts):
        """Parse various timestamp formats"""
        if isinstance(ts, datetime):
            return ts
        if isinstance(ts, str):
            # Try ISO format first
            try:
                return datetime.fromisoformat(ts.replace('Z', '+00:00'))
            except:
                pass
            # Try other common formats
            for fmt in ['%Y-%m-%d %H:%M:%S', '%Y-%m-%dT%H:%M:%S']:
                try:
                    return datetime.strptime(ts.split('.')[0], fmt)
                except:
                    continue
        return datetime.utcnow()  # Fallback

    # Sort events by timestamp
    sorted_events = sorted(events, key=lambda x: parse_timestamp(x.get('timestamp', x.get('published_at', x.get('ingested_at', '')))))

    # Group by time windows
    windows = defaultdict(list)
    for event in sorted_events:
        timestamp = parse_timestamp(event.get('timestamp', event.get('published_at', event.get('ingested_at', ''))))
        window_key = timestamp.replace(minute=0, second=0, microsecond=0)
        windows[window_key].append(event)

    # Detect correlations within windows
    correlated_groups = []

    for window_time, window_events in windows.items():
        if len(window_events) < 2:
            continue

        # Analyze correlations
        correlations = []

        # Check for location overlaps
        locations = defaultdict(list)
        for event in window_events:
            entities = event.get('entities', {})
            if isinstance(entities, str):
                try:
                    entities = json.loads(entities)
                except:
                    entities = {}

            event_locations = entities.get('locations', [])
            for loc in event_locations:
                if loc:
                    locations[loc].append(event)

        # Check for keyword overlaps
        keywords_overlap = defaultdict(list)
        for event in window_events:
            keywords = event.get('keywords', [])
            if isinstance(keywords, str):
                try:
                    keywords = json.loads(keywords)
                except:
                    keywords = []

            for keyword in keywords[:5]:  # Top 5 keywords
                keywords_overlap[keyword].append(event)

        # Check for cluster overlaps
        clusters = defaultdict(list)
        for event in window_events:
            cluster = event.get('event_cluster', 'general')
            clusters[cluster].append(event)

        # Create correlation groups
        # Group by location if multiple events share location
        for location, loc_events in locations.items():
            if len(loc_events) >= 2:
                correlated_groups.append({
                    'type': 'location_correlation',
                    'window_time': window_time.isoformat(),
                    'location': location,
                    'event_count': len(loc_events),
                    'events': loc_events,
                    'correlation_score': min(len(loc_events) * 0.3, 1.0)
                })

        # Group by keywords if shared across events
        for keyword, kw_events in keywords_overlap.items():
            if len(kw_events) >= 2 and len(kw_events) < len(window_events):  # Not all events
                correlated_groups.append({
                    'type': 'keyword_correlation',
                    'window_time': window_time.isoformat(),
                    'keyword': keyword,
                    'event_count': len(kw_events),
                    'events': kw_events,
                    'correlation_score': min(len(kw_events) * 0.2, 1.0)
                })

        # Group by event cluster
        for cluster, cluster_events in clusters.items():
            if len(cluster_events) >= 2:
                correlated_groups.append({
                    'type': 'cluster_correlation',
                    'window_time': window_time.isoformat(),
                    'cluster': cluster,
                    'event_count': len(cluster_events),
                    'events': cluster_events,
                    'correlation_score': min(len(cluster_events) * 0.25, 1.0)
                })

    # Sort by correlation score
    correlated_groups.sort(key=lambda x: x['correlation_score'], reverse=True)

    return correlated_groups[:10]  # Return top 10 correlations


# ─────────────────────────────────────────────────────────────────
# GEOSPATIAL EVENT CLUSTERING
# ─────────────────────────────────────────────────────────────────

def geospatial_clustering(events: list, eps_km: float = 100.0, min_samples: int = 2) -> dict:
    """
    Cluster events by geographic proximity using DBSCAN.
    Returns clusters with event counts and centroid locations.

    This function groups events that are geographically close to each other,
    helping identify geographic hotspots of activity.
    """
    import numpy as np
    from sklearn.cluster import DBSCAN
    from collections import defaultdict

    if not events:
        return {}

    # Extract coordinates (lat, lon) from events
    coordinates = []
    event_mapping = []

    for event in events:
        lat = None
        lon = None

        # Direct coordinates
        if 'latitude' in event and 'longitude' in event:
            try:
                lat = float(event['latitude'])
                lon = float(event['longitude'])
            except (ValueError, TypeError):
                pass

        # Fallback: try to extract from entities
        if lat is None and 'entities' in event:
            entities = event.get('entities', {})
            if isinstance(entities, str):
                try:
                    entities = json.loads(entities)
                except:
                    entities = {}

            # If we had geocoding, we could use location entities here
            # For now, skip events without direct coordinates
            continue

        if lat is not None and lon is not None:
            coordinates.append([lat, lon])
            event_mapping.append(event)

    if len(coordinates) < min_samples:
        return {}

    # Convert to numpy array
    coords_array = np.array(coordinates)

    # Convert to radians for haversine distance
    coords_rad = np.radians(coords_array)

    # Cluster using DBSCAN with haversine distance
    db = DBSCAN(eps=eps_km/6371.0, min_samples=min_samples, metric='haversine')
    labels = db.fit_predict(coords_rad)

    # Organize results
    cluster_groups = defaultdict(list)

    for idx, label in enumerate(labels):
        if label != -1:  # Ignore noise points
            cluster_groups[label].append(event_mapping[idx])

    # Calculate cluster centroids and statistics
    enhanced_clusters = {}

    for cluster_id, cluster_events in cluster_groups.items():
        # Get coordinates for this cluster
        cluster_coords = coords_array[labels == cluster_id]

        # Calculate centroid
        centroid_lat = np.mean(cluster_coords[:, 0])
        centroid_lon = np.mean(cluster_coords[:, 1])

        enhanced_clusters[f"cluster_{cluster_id}"] = {
            'cluster_id': cluster_id,
            'event_count': len(cluster_events),
            'centroid': {'latitude': centroid_lat, 'longitude': centroid_lon},
            'events': cluster_events,
            'radius_km': calculate_cluster_radius(cluster_coords, centroid_lat, centroid_lon)
        }

    return enhanced_clusters


def calculate_cluster_radius(coordinates, centroid_lat, centroid_lon):
    """Calculate the maximum distance from centroid to any point in the cluster"""
    import numpy as np

    max_distance = 0.0
    centroid = np.array([centroid_lat, centroid_lon])

    for coord in coordinates:
        # Haversine distance
        lat1, lon1 = np.radians(centroid)
        lat2, lon2 = np.radians(coord)

        dlat = lat2 - lat1
        dlon = lon2 - lon1

        a = np.sin(dlat/2)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2)**2
        c = 2 * np.arcsin(np.sqrt(a))
        radius = 6371.0 * c  # Earth's radius in km

        max_distance = max(max_distance, radius)

    return max_distance


# ─────────────────────────────────────────────────────────────────
# DYNAMIC EVENT CLASSIFICATION
# ─────────────────────────────────────────────────────────────────

# Dynamic keyword learning
_dynamic_clusters = defaultdict(int)  # cluster -> keyword_count
_cluster_keywords = defaultdict(lambda: defaultdict(int))  # cluster -> keyword -> count


def dynamic_event_classification(text: str, confidence_threshold: float = 0.3) -> tuple:
    """
    Dynamically classify events using both static rules and learned patterns.
    Returns (cluster_name, confidence_score).

    This function combines the static event clusters with dynamic learning
    from new keyword patterns.
    """
    global _dynamic_clusters, _cluster_keywords

    if not text:
        return "general", 0.0

    text_lower = text.lower()

    # First try static classification
    static_cluster = classify_event(text)

    # Extract keywords from text
    keywords = preprocess(text)

    # Update dynamic keyword statistics
    if keywords:
        for keyword in keywords[:10]:  # Top 10 keywords
            _cluster_keywords[static_cluster][keyword] += 1

    # Check if we have enough data for dynamic classification
    total_keywords = sum(sum(kw_dict.values()) for kw_dict in _cluster_keywords.values())

    if total_keywords > 1000:  # Only use dynamic classification with sufficient data
        # Score each cluster based on keyword matches
        cluster_scores = defaultdict(float)

        for keyword in keywords:
            for cluster, kw_dict in _cluster_keywords.items():
                if keyword in kw_dict:
                    # TF-like scoring
                    keyword_freq = kw_dict[keyword]
                    cluster_total = sum(kw_dict.values())
                    score = keyword_freq / cluster_total
                    cluster_scores[cluster] += score

        if cluster_scores:
            # Get best dynamic cluster
            best_dynamic_cluster = max(cluster_scores, key=cluster_scores.get)
            dynamic_score = cluster_scores[best_dynamic_cluster]

            # Use dynamic if confident enough
            if dynamic_score > confidence_threshold:
                # Update static cluster if dynamic is better
                if best_dynamic_cluster != static_cluster:
                    _dynamic_clusters[best_dynamic_cluster] += 1

                return best_dynamic_cluster, min(dynamic_score, 1.0)

    return static_cluster, 0.5  # Default confidence for static classification


def get_top_dynamic_clusters(n: int = 5) -> list:
    """Get the top dynamically learned clusters with their statistics"""
    global _dynamic_clusters

    sorted_clusters = sorted(_dynamic_clusters.items(), key=lambda x: x[1], reverse=True)
    return sorted_clusters[:n]


def reset_dynamic_learning():
    """Reset dynamic learning statistics (useful for testing or fresh starts)"""
    global _dynamic_clusters, _cluster_keywords
    _dynamic_clusters = defaultdict(int)
    _cluster_keywords = defaultdict(lambda: defaultdict(int))