import re
import math
import json
from collections import Counter, defaultdict
from typing import Optional, List, Dict, Any

# ─────────────────────────────────────────────────────────────────
# spaCy for NER
# ─────────────────────────────────────────────────────────────────
try:
    import spacy
    nlp = spacy.load("en_core_web_sm")
    SPACY_AVAILABLE = True
except Exception:
    SPACY_AVAILABLE = False
    print("[NLP] spaCy not available — NER disabled. Run: pip install spacy && python -m spacy download en_core_web_sm")

try:
    from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
    VADER_AVAILABLE = True
    vader_analyzer = SentimentIntensityAnalyzer()
except ImportError:
    VADER_AVAILABLE = False
    vader_analyzer = None
    print("[NLP] VADER not available.")

# ─────────────────────────────────────────────────────────────────
# STOPWORDS & CONFIG
# ─────────────────────────────────────────────────────────────────
STOPWORDS = {
    "i", "me", "my", "myself", "we", "our", "ours", "ourselves", "you", "your", 
    "yours", "yourself", "yourselves", "he", "him", "his", "himself", "she", 
    "her", "hers", "herself", "it", "its", "itself", "they", "them", "their", 
    "theirs", "themselves", "what", "which", "who", "whom", "this", "that", 
    "these", "those", "am", "is", "are", "was", "were", "be", "been", "being", 
    "have", "has", "had", "having", "do", "does", "did", "doing", "a", "an", 
    "the", "and", "but", "if", "or", "because", "as", "until", "while", "of", 
    "at", "by", "for", "with", "about", "against", "between", "into", "through", 
    "during", "before", "after", "above", "below", "to", "from", "up", "down", 
    "in", "out", "on", "off", "over", "under", "again", "further", "then", 
    "once", "here", "there", "when", "where", "why", "how", "all", "any", 
    "both", "each", "few", "more", "most", "other", "some", "such", "no", 
    "nor", "not", "only", "own", "same", "so", "than", "too", "very", "s", 
    "t", "can", "will", "just", "don", "should", "now"
}

# ─────────────────────────────────────────────────────────────────
# EVENT CLUSTERS
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
    "finance":    ["finance", "market", "nasdaq", "s&p", "stock", "shares", "dividend", "earnings"]
}

KEYWORD_TO_CLUSTER = {}
for cluster, keywords in EVENT_CLUSTERS.items():
    for kw in keywords:
        KEYWORD_TO_CLUSTER[kw] = cluster


def preprocess(text: Optional[str]) -> List[str]:
    if not text:
        return []
    text = text.lower()
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    tokens = text.split()
    return [t for t in tokens if len(t) > 2 and t not in STOPWORDS]

def analyze_sentiment(text: str) -> str:
    if not text or not VADER_AVAILABLE:
        return "neutral"
    try:
        scores = vader_analyzer.polarity_scores(str(text))
        if scores['compound'] >= 0.05:
            return "positive"
        elif scores['compound'] <= -0.05:
            return "negative"
        else:
            return "neutral"
    except Exception:
        return "neutral"

def extract_keywords(text: Optional[str], top_n: int = 5) -> List[str]:
    tokens = preprocess(text)
    freq = Counter(tokens)
    return [w for w, _ in freq.most_common(top_n)]

def extract_entities(text: Optional[str]) -> Dict[str, List[str]]:
    result = {"locations": [], "people": [], "orgs": [], "countries": []}
    if not text or not SPACY_AVAILABLE:
        return result

    doc = nlp(text[:1000])

    for ent in doc.ents:
        label = ent.label_
        name  = ent.text.strip()
        if label in ("GPE", "LOC"):
            result["locations"].append(name)
            result["countries"].append(name)
        elif label == "PERSON":
            result["people"].append(name)
        elif label == "ORG":
            result["orgs"].append(name)

    for k in result:
        result[k] = list(set(result[k]))

    return result

def classify_event(text: Optional[str]) -> str:
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

def enrich_event(doc: Dict[str, Any]) -> Dict[str, Any]:
    text = " ".join(filter(None, [
        doc.get("title", ""),
        doc.get("description", ""),
        doc.get("content", ""),
        doc.get("comment", "")
    ]))

    doc["clean_text"] = preprocess(text)
    doc["word_count"] = len(doc["clean_text"])
    
    doc["entities"] = extract_entities(text)
    doc["event_cluster"] = classify_event(text)
    doc["keywords"] = extract_keywords(text)
    doc["sentiment"] = analyze_sentiment(text)
    doc["confidence_score"] = 0.85 # Simplified for backend

    return doc
