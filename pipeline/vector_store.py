"""Weaviate vector store for semantic search over journal entries."""
from datetime import datetime
from typing import List, Optional

from sentence_transformers import SentenceTransformer

from config import WEAVIATE_CLASS_NAME, EMBEDDING_MODEL, WEAVIATE_URL


class VectorStore:
    """Semantic search over journal entries using Weaviate."""

    def __init__(
        self,
        url: Optional[str] = None,
        collection_name: str = WEAVIATE_CLASS_NAME,
        embedding_model: str = EMBEDDING_MODEL,
    ):
        url = url or WEAVIATE_URL
        self.collection_name = collection_name
        self.embedding_model = SentenceTransformer(embedding_model)

        try:
            import weaviate
            self.client = weaviate.Client(url)
            self._ensure_schema()
        except Exception as e:
            # Expose the underlying error message to help debugging
            raise RuntimeError(
                f"Weaviate non disponible ({url}) : {e}"
            ) from e

    def _ensure_schema(self):
        """Create collection if it doesn't exist (vectorizer=none, we provide vectors)."""
        schema = self.client.schema.get()
        classes = [c["class"] for c in schema.get("classes", [])]
        if self.collection_name in classes:
            return

        try:
            self.client.schema.create_class({
                "class": self.collection_name,
                "vectorizer": "none",
                "properties": [
                    {"name": "text", "dataType": ["text"]},
                    {"name": "entry_id", "dataType": ["string"]},
                    {"name": "timestamp", "dataType": ["date"]},
                    {"name": "entity_count", "dataType": ["int"]},
                    {"name": "entities", "dataType": ["string[]"]},
                ],
            })
        except Exception as e:
            # If the class already exists (422), ignore and continue
            msg = str(e)
            if "already exists" not in msg:
                raise

    def _embed(self, text: str) -> List[float]:
        """Generate embedding for text."""
        return self.embedding_model.encode(text).tolist()

    def add_entry(
        self,
        entry_id: str,
        text: str,
        timestamp: Optional[datetime] = None,
        metadata: Optional[dict] = None,
    ):
        """Add a journal entry to the vector store."""
        meta = metadata or {}
        ts = timestamp or datetime.now()
        # Weaviate "date" type expects RFC3339 string (no microseconds, with 'Z')
        ts_rfc3339 = ts.replace(microsecond=0).isoformat() + "Z"

        props = {
            "text": text[:8000],
            "entry_id": entry_id,
            "timestamp": ts_rfc3339,
            "entity_count": meta.get("entity_count", 0),
            "entities": meta.get("entities", [])[:20],
        }

        embedding = self._embed(text)

        self.client.data_object.create(
            data_object=props,
            class_name=self.collection_name,
            vector=embedding,
            uuid=entry_id,
        )

    def search(self, query: str, n_results: int = 5) -> List[dict]:
        """Semantic search over journal entries."""
        query_embedding = self._embed(query)

        result = (
            self.client.query
            .get(self.collection_name, ["text", "entry_id", "timestamp", "entities"])
            .with_near_vector({"vector": query_embedding})
            .with_limit(n_results)
            .do()
        )

        data = result.get("data", {}).get("Get", {}).get(self.collection_name, [])
        if not data:
            return []

        return [
            {
                "id": obj.get("entry_id", ""),
                "text": obj.get("text", ""),
                "metadata": {
                    "entry_id": obj.get("entry_id"),
                    "timestamp": obj.get("timestamp"),
                    "entities": obj.get("entities", []),
                },
                "distance": None,
            }
            for obj in data
        ]

    def count(self) -> int:
        """Number of entries in the collection."""
        result = self.client.query.aggregate(self.collection_name).with_meta_count().do()
        agg = result.get("data", {}).get("Aggregate", {}).get(self.collection_name, [])
        return agg[0].get("meta", {}).get("count", 0) if agg else 0
