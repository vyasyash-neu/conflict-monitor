import os
from sentence_transformers import SentenceTransformer
from qdrant_client import QdrantClient

QDRANT_URL = os.getenv("QDRANT_URL", "http://localhost:6333")
_model = None
_qdrant = None

def init_vectorizer():
    global _model, _qdrant
    _model = SentenceTransformer("all-MiniLM-L6-v2")
    _qdrant = QdrantClient(url=QDRANT_URL)

def get_model():
    return _model

def get_qdrant():
    return _qdrant

def embed_query(text: str) -> list:
    return _model.encode(text).tolist()