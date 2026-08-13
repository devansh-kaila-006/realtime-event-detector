import json
import pandas as pd
from typing import Iterator

from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.types import StringType

# Import our pure Python NLP functions from the backend to avoid duplicating logic
# We'll adapt them for Pandas UDFs
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from backend.nlp import classify_event, analyze_sentiment, extract_entities, extract_keywords

@pandas_udf(StringType())
def vectorize_sentiment(texts: pd.Series) -> pd.Series:
    """Batch process sentiment analysis using Pandas UDF for performance."""
    return texts.apply(lambda t: analyze_sentiment(t) if pd.notnull(t) else 'neutral')

@pandas_udf(StringType())
def vectorize_classification(texts: pd.Series) -> pd.Series:
    """Batch process event classification using Pandas UDF."""
    return texts.apply(lambda t: classify_event(t) if pd.notnull(t) else 'general')

@pandas_udf(StringType())
def vectorize_entities(texts: pd.Series) -> pd.Series:
    """Batch process entity extraction."""
    return texts.apply(lambda t: json.dumps(extract_entities(t)) if pd.notnull(t) else '{}')

@pandas_udf(StringType())
def vectorize_keywords(texts: pd.Series) -> pd.Series:
    """Batch process keyword extraction."""
    return texts.apply(lambda t: json.dumps(extract_keywords(t)) if pd.notnull(t) else '[]')
