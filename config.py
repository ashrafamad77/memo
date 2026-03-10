"""Configuration for the Personal Memory Pipeline."""
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# Paths
PROJECT_ROOT = Path(__file__).parent
DATA_DIR = PROJECT_ROOT / "data"
CHROMA_DIR = DATA_DIR / "chroma"

# Neo4j (local default - use Docker: docker run -p 7474:7474 -p 7687:7687 neo4j)
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "password")

# Vector Store (Weaviate)
CHROMA_COLLECTION = "journal_entries"  # collection name (reused)
WEAVIATE_URL = os.getenv("WEAVIATE_URL", "http://localhost:8080")

# spaCy model (install separately: python -m spacy download fr_core_news_sm)
SPACY_MODEL = os.getenv("SPACY_MODEL", "fr_core_news_sm")

# Embedding model (multilingual, works for FR/EN)
EMBEDDING_MODEL = "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"

# OpenAI (for LLM-based extraction)
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
