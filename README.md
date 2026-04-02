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
USER_NAME=Ashraf

AZURE_OPENAI_API_KEY=ta-clé-azure
AZURE_OPENAI_ENDPOINT=https://TON-RESOURCE.cognitiveservices.azure.com/
AZURE_OPENAI_DEPLOYMENT=gpt-4o-mini
AZURE_OPENAI_API_VERSION=2024-12-01-preview

WEAVIATE_URL=http://localhost:8081
NEO4J_URI=bolt://localhost:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=password

# Optionnel — entity linking instances (lieux / personnes / orgs) via Babelfy uniquement
# BABELFY_API_KEY=
# MEMO_BABELFY_LANG=EN
# MEMO_BABELFY_E55=1   # Babelfy CONCEPTS → candidats Wikidata pour types E55 (0 = désactiver)

# Optionnel — grounding agentique via Wikidata Vector + pivot BabelNet (voir https://wd-vectordb.wmcloud.org/docs )
# MEMO_GROUNDING_MODE=auto   # auto | vector | legacy
# MEMO_WD_VECTOR_ALLOW_PUBLIC=1   # 1 = activer le chemin vector sans X-API-SECRET (API publique alpha)
# MEMO_WD_VECTOR_API_SECRET=
# MEMO_WD_VECTOR_BASE_URL=https://wd-vectordb.wmcloud.org
# MEMO_WD_VECTOR_K=10
# MEMO_WD_VECTOR_LANG=en
# MEMO_WD_VECTOR_RERANK=true
# MEMO_WD_VECTOR_SCORE_MARGIN=0.05
# MEMO_WD_VECTOR_MIN_SCORE=0.0
# MEMO_WD_VECTOR_INSTANCEOF_E55=   # QIDs séparés par des virgules (filtre instance of), idem E53 / E21 / E74
```

→ Azure : **Keys and endpoint** pour la clé et l’URL, **Deployments** pour le nom du déploiement. Weaviate par défaut sur le port 8081 (voir `docker-compose.yml`). **Grounding** : avec `MEMO_WD_VECTOR_API_SECRET` *ou* `MEMO_WD_VECTOR_ALLOW_PUBLIC=1`, et `MEMO_GROUNDING_MODE=auto` (défaut), le graphe agentique interroge l’API **Wikidata Vector** puis BabelNet ; en `legacy` ou sans ces options, Babelfy **CONCEPTS** + `getSenses`.

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

# Ajouter une entrée via workflow agentic (LangGraph)
python main.py add-agentic "Aujourd'hui j'ai déjeuné avec Marie à Paris. On a parlé du projet."

# Reset Neo4j (utile après changement de schéma)
python main.py reset-graph

# Reset Weaviate (vide les objets indexés)
python main.py reset-vector

# Reset complet (Neo4j + Weaviate)
python main.py reset-all

# Recherche sémantique
python main.py search "repas avec des amis" --n 5

# Recherche par entité (graph)
python main.py entity "Marie"

# Lister les entités connues
python main.py list --limit 20
```

## Notes de modélisation (important)

- **Neo4j**: on stocke *toutes* les entrées (`Entry`) avec `input_time`. Une activité canonique (`E7_Activity`) est créée/mergée via une clé (bucket jour + entités clés) et les entrées pointent vers elle via `(:Entry)-[:P67_refers_to {ref_type:'about_activity'}]->(:E7_Activity)`.
- **Weaviate**: on indexe pour la recherche sémantique, mais on évite les doublons exacts **le même jour** via `content_hash + day` (si déjà présent, on skip l'insertion vector).
- **Métadonnées**: `event_type` / `emotions` sont stockés en metadata (pas mélangés aux entités littérales) afin d'éviter d’avoir des mots “inférés” dans `entities`.

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
