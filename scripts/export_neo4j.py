#!/usr/bin/env python3
"""Export all Neo4j nodes and relationships to JSON."""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from neo4j import GraphDatabase
from config import NEO4J_PASSWORD, NEO4J_URI, NEO4J_USER


def _serialize(node):
    d = dict(node)
    d["_labels"] = list(node.labels)
    d["_id"] = node.element_id
    return d


def _serialize_rel(rel):
    return {
        "type": rel.type,
        "start": rel.start_node.element_id,
        "end": rel.end_node.element_id,
        "properties": dict(rel),
    }


def main():
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    with driver.session() as s:
        nodes = [dict(r)["n"] for r in s.run("MATCH (n) RETURN n")]
        rels = [dict(r)["r"] for r in s.run("MATCH (n)-[r]->(m) RETURN r")]

    out = {
        "nodes": [_serialize(n) for n in nodes],
        "relationships": [_serialize_rel(r) for r in rels],
    }
    path = Path(__file__).parent.parent / "data" / "neo4j_export.json"
    path.parent.mkdir(exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2, ensure_ascii=False, default=str)

    print(f"Exported {len(out['nodes'])} nodes, {len(out['relationships'])} relationships → {path}")


if __name__ == "__main__":
    main()
