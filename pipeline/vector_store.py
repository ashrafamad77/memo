"""Weaviate vector store for semantic search over journal entries."""
from datetime import datetime
import hashlib
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
            # Best effort schema migration: add missing properties if needed
            try:
                existing = next(c for c in schema.get("classes", []) if c.get("class") == self.collection_name)
                existing_props = {p.get("name") for p in existing.get("properties", [])}
                desired = [
                    ("day", ["string"]),
                    ("content_hash", ["string"]),
                    ("emotions", ["string[]"]),
                    ("event_type", ["string"]),
                ]
                for name, dtype in desired:
                    if name not in existing_props:
                        self.client.schema.property.create(
                            class_name=self.collection_name,
                            property={"name": name, "dataType": dtype},
                        )
            except Exception:
                pass
            return

        try:
            self.client.schema.create_class({
                "class": self.collection_name,
                "vectorizer": "none",
                "properties": [
                    {"name": "text", "dataType": ["text"]},
                    {"name": "entry_id", "dataType": ["string"]},
                    {"name": "timestamp", "dataType": ["date"]},
                    {"name": "day", "dataType": ["string"]},
                    {"name": "content_hash", "dataType": ["string"]},
                    {"name": "entity_count", "dataType": ["int"]},
                    {"name": "entities", "dataType": ["string[]"]},
                    {"name": "emotions", "dataType": ["string[]"]},
                    {"name": "event_type", "dataType": ["string"]},
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

        day = ts.date().isoformat()
        normalized = " ".join((text or "").strip().split()).lower()
        content_hash = hashlib.sha256(normalized.encode("utf-8")).hexdigest()

        # Skip vector insertion if exact duplicate already inserted today
        try:
            where = {
                "operator": "And",
                "operands": [
                    {"path": ["day"], "operator": "Equal", "valueString": day},
                    {"path": ["content_hash"], "operator": "Equal", "valueString": content_hash},
                ],
            }
            existing = (
                self.client.query.get(self.collection_name, ["entry_id"])
                .with_where(where)
                .with_limit(1)
                .do()
            )
            hits = existing.get("data", {}).get("Get", {}).get(self.collection_name, [])
            if hits:
                return
        except Exception:
            pass

        llm_meta = meta.get("metadata") if isinstance(meta.get("metadata"), dict) else {}
        emotions = llm_meta.get("emotions", [])
        emotions = [e for e in emotions if isinstance(e, str)][:10] if isinstance(emotions, list) else []
        event_type = llm_meta.get("event_type")
        event_type = str(event_type)[:64] if isinstance(event_type, str) else ""

        props = {
            "text": text[:8000],
            "entry_id": entry_id,
            "timestamp": ts_rfc3339,
            "day": day,
            "content_hash": content_hash,
            "entity_count": meta.get("entity_count", 0),
            "entities": meta.get("entities", [])[:20],
            "emotions": emotions,
            "event_type": event_type,
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

    def reset_vector(self) -> None:
        """Delete all objects in the class (keeps schema)."""
        where = {"path": ["entry_id"], "operator": "Like", "valueString": "*"}
        # Batch delete all objects in this class
        self.client.batch.delete_objects(
            class_name=self.collection_name,
            where=where,
            output="minimal",
        )
