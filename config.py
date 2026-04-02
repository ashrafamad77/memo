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

# Embedding model id (reference; matches Docker image t2v-transformers / Weaviate config)
EMBEDDING_MODEL = "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"
# Bare-metal Python → sidecar published on host (docker-compose maps 8082:8080)
EMBEDDING_INFERENCE_URL = os.getenv("EMBEDDING_INFERENCE_URL", "http://127.0.0.1:8082")
# Weaviate transformers-inference uses POST /vectors + {"text": "..."} per official docs
EMBEDDING_INFERENCE_PATH = os.getenv("EMBEDDING_INFERENCE_PATH", "/vectors")
EMBEDDING_VECTOR_DIM = int(os.getenv("EMBEDDING_VECTOR_DIM", "384"))

# User (owner of the journal – first-person "je" maps to this)
USER_NAME = os.getenv("USER_NAME", "")  # e.g. Ashraf
USER_AGE = os.getenv("USER_AGE", "")  # optional
USER_LANG = os.getenv("USER_LANG", "")  # optional, e.g. french

# CORS – frontend origin (port 3000). If you open the UI as http://<LAN-IP>:3000 (Next uses -H 0.0.0.0),
# add that exact origin here, e.g. CORS_ORIGINS=http://192.168.1.42:3000,http://localhost:3000
CORS_ORIGINS = os.getenv(
    "CORS_ORIGINS",
    "http://localhost:3000,http://127.0.0.1:3000,http://88.223.92.163:3000",
)

# LLM extraction (Azure AI Foundry)
AZURE_OPENAI_API_KEY = os.getenv("AZURE_OPENAI_API_KEY", "")
AZURE_OPENAI_ENDPOINT = os.getenv("AZURE_OPENAI_ENDPOINT", "")  # e.g. https://xxx.cognitiveservices.azure.com/
AZURE_OPENAI_DEPLOYMENT = os.getenv("AZURE_OPENAI_DEPLOYMENT", "")  # deployment name, e.g. gpt-4o-mini
AZURE_OPENAI_API_VERSION = os.getenv("AZURE_OPENAI_API_VERSION", "2024-12-01-preview")

# Babelfy HTTP API 1.0 — https://babelfy.org/ (GET + Accept-Encoding: gzip; key shared with BabelNet).
# Required API params: text, lang, key. Optional: annType, annRes, match, th, MCS, dens, cands, posTag, extAIDA.
BABELFY_API_KEY = os.getenv("BABELFY_API_KEY", "")
# Babelfy ``lang`` (e.g. EN, FR, or AGNOSTIC per API docs).
MEMO_BABELFY_LANG = os.getenv("MEMO_BABELFY_LANG", "EN")
# ``annRes``: empty = omit (same as official minimal samples / requests example). WIKI | WN | BABELNET if you must set it.
MEMO_BABELFY_ANN_RES = os.getenv("MEMO_BABELFY_ANN_RES", "").strip()
# Optional Babelfy GET parameters (empty = omit that knob; Babelfy uses API defaults).
# match: EXACT_MATCHING | PARTIAL_MATCHING — default keeps previous pipeline behavior.
MEMO_BABELFY_MATCH = os.getenv("MEMO_BABELFY_MATCH", "PARTIAL_MATCHING").strip()
# th: cutting threshold (float, Babelfy ``th``).
MEMO_BABELFY_TH = os.getenv("MEMO_BABELFY_TH", "").strip()
# MCS: backoff (API-specific; often true / false).
MEMO_BABELFY_MCS = os.getenv("MEMO_BABELFY_MCS", "").strip()
# dens: densest-subgraph heuristic (true / false).
MEMO_BABELFY_DENS = os.getenv("MEMO_BABELFY_DENS", "").strip()
# cands: candidate list mode (e.g. TOP vs ALL — see Babelfy docs).
MEMO_BABELFY_CANDS = os.getenv("MEMO_BABELFY_CANDS", "").strip()
# posTag: tokenization / POS pipeline id.
MEMO_BABELFY_POS_TAG = os.getenv("MEMO_BABELFY_POS_TAG", "").strip()
# extAIDA: extend candidates with aida_means (true / false).
MEMO_BABELFY_EXT_AIDA = os.getenv("MEMO_BABELFY_EXT_AIDA", "").strip()
# Use Babelfy CONCEPTS pass to suggest Wikidata classes for E55 types (1 = on when BABELFY_API_KEY set)
MEMO_BABELFY_E55 = os.getenv("MEMO_BABELFY_E55", "1")
