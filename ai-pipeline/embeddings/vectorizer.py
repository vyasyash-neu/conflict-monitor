import logging
from sentence_transformers import SentenceTransformer
from qdrant_client import QdrantClient
from qdrant_client.models import VectorParams, Distance

log = logging.getLogger(__name__)

COLLECTION_NAME = "conflict_events"
EMBEDDING_DIM = 384  # all-MiniLM-L6-v2 output dimension


class EventVectorizer:
    def __init__(self, qdrant_url: str):
        log.info("📦 Loading embedding model (all-MiniLM-L6-v2)...")
        self.model = SentenceTransformer("all-MiniLM-L6-v2")
        self.qdrant = QdrantClient(url=qdrant_url)
        self._ensure_collection()
        log.info("✅ Vectorizer ready")

    def _ensure_collection(self):
        collections = [c.name for c in self.qdrant.get_collections().collections]
        if COLLECTION_NAME not in collections:
            self.qdrant.create_collection(
                collection_name=COLLECTION_NAME,
                vectors_config=VectorParams(
                    size=EMBEDDING_DIM,
                    distance=Distance.COSINE,
                ),
            )
            log.info(f"📦 Created Qdrant collection: {COLLECTION_NAME}")

    def embed(self, enriched_event: dict) -> list:
        """Generate embedding from event summary + context."""
        text = self._build_text(enriched_event)
        embedding = self.model.encode(text).tolist()
        return embedding

    def _build_text(self, event: dict) -> str:
        """Build a rich text representation for embedding."""
        parts = []
        if event.get("summary"):
            parts.append(event["summary"])
        if event.get("category"):
            parts.append(f"Category: {event['category']}")
        if event.get("sub_type"):
            parts.append(f"Type: {event['sub_type']}")
        if event.get("location_name"):
            parts.append(f"Location: {event['location_name']}")
        if event.get("country"):
            parts.append(f"Country: {event['country']}")
        if event.get("actor1"):
            parts.append(f"Actor: {event['actor1']}")
        if event.get("actor2"):
            parts.append(f"Target: {event['actor2']}")
        return " | ".join(parts)