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

Variables importantes :

**Option A — OpenAI**
```env
OPENAI_API_KEY=...
OPENAI_MODEL=gpt-4o-mini
```

**Option B — Azure AI Foundry** (prioritaire si les deux sont définis)
```env
AZURE_OPENAI_API_KEY=ta-clé-azure
AZURE_OPENAI_ENDPOINT=https://TON-RESOURCE.openai.azure.com/openai/v1
AZURE_OPENAI_DEPLOYMENT=gpt-4o-mini
```
→ Dans Azure AI Foundry : **Keys and endpoint** pour la clé et l’URL, **Deployments** pour le nom du déploiement (ex. `gpt-4o-mini`).

**Weaviate + Neo4j**
```env
WEAVIATE_URL=http://localhost:8080
NEO4J_URI=bolt://localhost:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=password
```

Notes :
- Si tu utilises **Azure AI Foundry**, mets dans `.env` : `AZURE_OPENAI_API_KEY`, `AZURE_OPENAI_ENDPOINT` et `AZURE_OPENAI_DEPLOYMENT` (nom du déploiement, ex. `gpt-4o-mini`). La pipeline utilisera Azure en priorité.
- Si ton Weaviate est sur une autre machine, mets son IP/host dans `WEAVIATE_URL` (ex: `http://88.223.92.163:8080`).
- Si tu vois une erreur OpenAI `429 insufficient_quota`, utilise Azure ou ajoute du crédit côté OpenAI.

## Services (Neo4j + Weaviate)

Lance les services avec Docker (si tu as un `docker-compose.yml` configuré) :

```bash
docker-compose up -d
```

Par défaut :
- **Weaviate** : `http://localhost:8080`
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
- **Aucune entité extraite / erreur OpenAI**
  - Si tu vois `429 insufficient_quota`, ton quota OpenAI est épuisé
  - Vérifie que `OPENAI_API_KEY` est bien chargé (re-lancer le shell ou `source .venv/bin/activate`)
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
