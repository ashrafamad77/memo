# Personal Memory Pipeline — PoC

Pipeline minimal : **texte → extraction d'entités (LLM) → Neo4j + Weaviate (vector store)**.

## Prérequis

- Python 3.10+
- Docker (recommandé, pour Neo4j + Weaviate)

## Installation

```bash
cd memo
python -m venv .venv
.venv\Scripts\activate   # Windows
# source .venv/bin/activate  # Linux/Mac

pip install -r requirements.txt
```

## Configuration (.env)

Crée un fichier `.env` à la racine.

```env
AZURE_OPENAI_API_KEY=ta-clé-azure
AZURE_OPENAI_ENDPOINT=https://TON-RESOURCE.cognitiveservices.azure.com/
AZURE_OPENAI_DEPLOYMENT=gpt-4o-mini
AZURE_OPENAI_API_VERSION=2024-12-01-preview

WEAVIATE_URL=http://localhost:8081
NEO4J_URI=bolt://localhost:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=password
```

→ Azure : **Keys and endpoint** pour la clé et l’URL, **Deployments** pour le nom du déploiement. Weaviate par défaut sur le port 8081 (voir `docker-compose.yml`).

## Services (Neo4j + Weaviate)

Lance les services avec Docker (si tu as un `docker-compose.yml` configuré) :

```bash
docker-compose up -d
```

Par défaut :
- **Weaviate** : `http://localhost:8081` (port 8081 pour éviter conflit avec d’autres services)
- **Neo4j** : `bolt://localhost:7687` (user `neo4j`, password `password`)

## Utilisation

```bash
# Ajouter une entrée journal
python main.py add "Aujourd'hui j'ai déjeuné avec Marie à Paris. On a parlé du projet."

# Recherche sémantique
python main.py search "repas avec des amis" --n 5

# Recherche par entité (graph)
python main.py entity "Marie"

# Lister les entités connues
python main.py list --limit 20
```

## Dépannage rapide

- **Vector: init-error: Weaviate non disponible (...)**
  - Vérifie `WEAVIATE_URL` (localhost vs IP serveur)
  - Vérifie que le conteneur Weaviate tourne (`docker ps`)
- **Aucune entité extraite / erreur Azure**
  - Vérifie `AZURE_OPENAI_API_KEY`, `AZURE_OPENAI_ENDPOINT` et `AZURE_OPENAI_DEPLOYMENT` dans `.env`
  - Recharge le shell : `source .venv/bin/activate`
- **Pourquoi “BertModel LOAD REPORT … MiniLM” ?**
  - C’est le modèle d’**embeddings** (SentenceTransformers) utilisé pour la recherche sémantique (Weaviate).
  - Ce n’est pas le NER : l’extraction d’entités est faite par le LLM.

## Structure

```
memo/
├── pipeline/
│   ├── llm_extractor.py # Extraction LLM (JSON entités)
│   ├── graph_store.py  # Neo4j
│   ├── vector_store.py # Weaviate + embeddings
│   └── pipeline.py     # Orchestration
├── main.py             # CLI
├── config.py
├── requirements.txt
└── docker-compose.yml  # Neo4j + Weaviate (si présent)
```

## Données

- **Weaviate** : objets et vecteurs stockés dans Weaviate (classe `journal_entries`)
- **Neo4j** : voir docker-compose ou Neo4j Desktop
