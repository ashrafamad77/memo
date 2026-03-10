# Personal Memory Pipeline — PoC

Pipeline minimal : **texte → extraction d'entités → Neo4j + ChromaDB**.

## Prérequis

- Python 3.10+
- Docker (optionnel, pour Neo4j)

## Installation

```bash
cd personal-memory-poc
python -m venv .venv
.venv\Scripts\activate   # Windows
# source .venv/bin/activate  # Linux/Mac

pip install -r requirements.txt

# Modèle spaCy (français)
python -m spacy download fr_core_news_sm

# Ou anglais si besoin
# python -m spacy download en_core_web_sm
```

## Neo4j (optionnel)

Pour le graph store :

```bash
docker-compose up -d
```

Ou installe Neo4j Desktop. Par défaut : `bolt://localhost:7687`, user `neo4j`, password `password`.

Sans Neo4j, le pipeline fonctionne en mode vector-only (ChromaDB).

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

## Structure

```
personal-memory-poc/
├── pipeline/
│   ├── extractor.py    # NER spaCy (Person, Place, Date, etc.)
│   ├── graph_store.py  # Neo4j
│   ├── vector_store.py # ChromaDB + embeddings
│   └── pipeline.py     # Orchestration
├── main.py             # CLI
├── config.py
├── requirements.txt
└── docker-compose.yml  # Neo4j
```

## Données

- **ChromaDB** : `data/chroma/` (local)
- **Neo4j** : voir docker-compose ou Neo4j Desktop
