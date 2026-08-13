import json

# Initialize HuggingFace Pipelines inside the worker nodes (lazy loading)
_sentiment_pipeline = None
_embedder = None
_spacy_nlp = None

def get_sentiment_pipeline():
    global _sentiment_pipeline
    if _sentiment_pipeline is None:
        from transformers import pipeline
        # Use a lightweight fast model for streaming
        _sentiment_pipeline = pipeline("sentiment-analysis", model="distilbert-base-uncased-finetuned-sst-2-english", device=-1)
    return _sentiment_pipeline

def get_embedder():
    global _embedder
    if _embedder is None:
        from sentence_transformers import SentenceTransformer
        _embedder = SentenceTransformer('all-MiniLM-L6-v2')
    return _embedder

def get_spacy():
    global _spacy_nlp
    if _spacy_nlp is None:
        import spacy
        try:
            _spacy_nlp = spacy.load("en_core_web_sm")
        except OSError:
            from spacy.cli import download
            download("en_core_web_sm")
            _spacy_nlp = spacy.load("en_core_web_sm")
    return _spacy_nlp

def vectorize_sentiment(t: str) -> str:
    """Process sentiment analysis using Transformer (DistilBERT)."""
    if not t or not str(t).strip():
        return "NEUTRAL"
    pipe = get_sentiment_pipeline()
    # Truncate text to model limit
    res = pipe(str(t)[:512])[0]
    return res['label'] # 'POSITIVE' or 'NEGATIVE'

def vectorize_embeddings(t: str) -> str:
    """Generate dense vector embeddings using MiniLM for advanced clustering/correlation."""
    if not t or not str(t).strip():
        return "[]"
    model = get_embedder()
    emb = model.encode(str(t))
    return json.dumps(emb.tolist())

def vectorize_entities(t: str) -> str:
    """Process entity extraction using spaCy."""
    if not t or not str(t).strip():
        return "{}"
    nlp = get_spacy()
    doc = nlp(str(t)[:1000])
    entities = {}
    for ent in doc.ents:
        if ent.label_ in ["PERSON", "ORG", "GPE", "EVENT", "LOC"]:
            entities.setdefault(ent.label_, []).append(ent.text)
    return json.dumps(entities)

def vectorize_keywords(t: str) -> str:
    """Extract nouns and proper nouns as keywords."""
    if not t or not str(t).strip():
        return "[]"
    nlp = get_spacy()
    doc = nlp(str(t)[:1000])
    keywords = [token.text.lower() for token in doc if token.pos_ in ["NOUN", "PROPN"] and not token.is_stop]
    return json.dumps(list(set(keywords)))
