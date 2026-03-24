"""Configuration for the Personal Memory Pipeline."""
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# Paths
PROJECT_ROOT = Path(__file__).parent
DATA_DIR = PROJECT_ROOT / "data"

# Neo4j (local default - use Docker: docker run -p 7474:7474 -p 7687:7687 neo4j)
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "password")

# Vector Store (Weaviate) – default port 8081 matches docker-compose (8081:8080)
WEAVIATE_URL = os.getenv("WEAVIATE_URL", "http://localhost:8081")
WEAVIATE_CLASS_NAME = os.getenv("WEAVIATE_CLASS_NAME", "journal_entries")

# Embedding model (multilingual, works for FR/EN)
EMBEDDING_MODEL = "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"

# User (owner of the journal – first-person "je" maps to this)
USER_NAME = os.getenv("USER_NAME", "")  # e.g. Ashraf
USER_AGE = os.getenv("USER_AGE", "")  # optional
USER_LANG = os.getenv("USER_LANG", "")  # optional, e.g. french

# CORS – frontend origin (port 3000), same setup locally and on VPS
CORS_ORIGINS = os.getenv(
    "CORS_ORIGINS",
    "http://localhost:3000,http://127.0.0.1:3000,http://88.223.92.163:3000",
)

# LLM extraction (Azure AI Foundry)
AZURE_OPENAI_API_KEY = os.getenv("AZURE_OPENAI_API_KEY", "")
AZURE_OPENAI_ENDPOINT = os.getenv("AZURE_OPENAI_ENDPOINT", "")  # e.g. https://xxx.cognitiveservices.azure.com/
AZURE_OPENAI_DEPLOYMENT = os.getenv("AZURE_OPENAI_DEPLOYMENT", "")  # deployment name, e.g. gpt-4o-mini
AZURE_OPENAI_API_VERSION = os.getenv("AZURE_OPENAI_API_VERSION", "2024-12-01-preview")
