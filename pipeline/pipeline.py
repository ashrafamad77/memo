"""Main pipeline: text -> extract -> graph + vector store."""
from datetime import datetime
from typing import List, Optional
import uuid

from .extractor import EntityExtractor, ExtractionResult
from .graph_store import GraphStore
from .llm_extractor import LLMExtractor
from .vector_store import VectorStore

from config import (
    DATA_DIR,
    OPENAI_API_KEY,
    OPENAI_MODEL,
    SPACY_MODEL,
)


class MemoryPipeline:
    """
    Orchestrates the full pipeline:
    1. Extract entities from text (LLM si OPENAI_API_KEY, sinon spaCy/regex)
    2. Store in Neo4j (graph)
    3. Store in Weaviate (vector search)
    """
    
    def __init__(
        self,
        spacy_model: str = SPACY_MODEL,
        use_graph: bool = True,
        use_vector: bool = True,
        use_llm: Optional[bool] = None,
    ):
        if use_llm is None:
            use_llm = bool(OPENAI_API_KEY and OPENAI_API_KEY.strip())
        if use_llm and OPENAI_API_KEY:
            try:
                self.extractor = LLMExtractor(model=OPENAI_MODEL)
            except Exception:
                self.extractor = EntityExtractor(model_name=spacy_model)
        else:
            self.extractor = EntityExtractor(model_name=spacy_model)
        
        self.graph_store = None
        if use_graph:
            try:
                self.graph_store = GraphStore()
            except Exception:
                self.graph_store = None  # Neo4j non disponible
        
        self.vector_store = None
        if use_vector:
            try:
                self.vector_store = VectorStore()
            except Exception:
                self.vector_store = None  # Weaviate non disponible
    
    def process(self, text: str, entry_id: Optional[str] = None) -> dict:
        """
        Process a journal entry through the full pipeline.
        
        Returns: dict with entry_id, extraction result, and status.
        """
        entry_id = entry_id or str(uuid.uuid4())
        
        # 1. Extract entities
        extraction = self.extractor.extract(text)
        
        # 2. Store in graph (if Neo4j available)
        graph_status = "skipped"
        if self.graph_store:
            try:
                self.graph_store.store_entry(
                    entry_id=entry_id,
                    text=text,
                    extraction=extraction,
                )
                graph_status = "ok"
            except Exception as e:
                graph_status = f"error: {e}"
        
        # 3. Store in vector DB
        vector_status = "skipped"
        if self.vector_store:
            try:
                self.vector_store.add_entry(
                    entry_id=entry_id,
                    text=text,
                    metadata={
                        "entity_count": len(extraction.entities),
                        "entities": [e.text for e in extraction.entities[:10]],
                    },
                )
                vector_status = "ok"
            except Exception as e:
                vector_status = f"error: {e}"
        
        return {
            "entry_id": entry_id,
            "entities": [
                {"text": e.text, "type": EntityExtractor.LABEL_TO_TYPE.get(e.label, e.label)}
                for e in extraction.entities
            ],
            "graph": graph_status,
            "vector": vector_status,
        }
    
    def search_semantic(self, query: str, n_results: int = 5) -> List[dict]:
        """Semantic search over journal entries."""
        if not self.vector_store:
            return []
        return self.vector_store.search(query, n_results=n_results)
    
    def search_by_entity(self, entity_name: str) -> List[dict]:
        """Find entries that mention an entity (graph query)."""
        if not self.graph_store:
            return []
        return self.graph_store.search_by_entity(entity_name)
    
    def list_entities(self, limit: int = 50) -> List[dict]:
        """List all known entities from the graph."""
        if not self.graph_store:
            return []
        return self.graph_store.query_entities(limit=limit)
    
    def close(self):
        """Clean up connections."""
        if self.graph_store:
            self.graph_store.close()
