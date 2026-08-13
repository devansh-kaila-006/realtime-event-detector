import json
import pandas as pd
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.types import StringType

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

@pandas_udf(StringType())
def vectorize_sentiment(texts: pd.Series) -> pd.Series:
    """Batch process sentiment analysis using Transformer (DistilBERT)."""
    pipe = get_sentiment_pipeline()
    def _analyze(t):
        if not pd.notnull(t) or not str(t).strip():
            return "NEUTRAL"
        # Truncate text to model limit
        res = pipe(str(t)[:512])[0]
        return res['label'] # 'POSITIVE' or 'NEGATIVE'
    
    return texts.apply(_analyze)

@pandas_udf(StringType())
def vectorize_embeddings(texts: pd.Series) -> pd.Series:
    """Generate dense vector embeddings using MiniLM for advanced clustering/correlation."""
    model = get_embedder()
    valid_texts = texts.fillna("").tolist()
    # Batch encode is much faster
    embeddings = model.encode(valid_texts, batch_size=32, show_progress_bar=False)
    # Return as JSON string array so Spark can store it
    return pd.Series([json.dumps(emb.tolist()) for emb in embeddings])

@pandas_udf(StringType())
def vectorize_entities(texts: pd.Series) -> pd.Series:
    """Batch process entity extraction using spaCy."""
    nlp = get_spacy()
    def _extract(t):
        if not pd.notnull(t) or not str(t).strip():
            return "{}"
        doc = nlp(str(t)[:1000])
        entities = {}
        for ent in doc.ents:
            if ent.label_ in ["PERSON", "ORG", "GPE", "EVENT", "LOC"]:
                entities.setdefault(ent.label_, []).append(ent.text)
        return json.dumps(entities)
    
    return texts.apply(_extract)

@pandas_udf(StringType())
def vectorize_keywords(texts: pd.Series) -> pd.Series:
    """Extract nouns and proper nouns as keywords."""
    nlp = get_spacy()
    def _extract(t):
        if not pd.notnull(t) or not str(t).strip():
            return "[]"
        doc = nlp(str(t)[:1000])
        keywords = [token.text.lower() for token in doc if token.pos_ in ["NOUN", "PROPN"] and not token.is_stop]
        return json.dumps(list(set(keywords)))
    
    return texts.apply(_extract)
