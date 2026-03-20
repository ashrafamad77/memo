"""Type Resolver: ensures E55_Type reuse and name normalization."""
from typing import Any, Dict, List, Optional


class TypeResolver:
    """Queries existing E55_Type nodes and normalizes new type names."""

    def __init__(self, driver):
        self.driver = driver

    def get_existing_types(self) -> List[str]:
        with self.driver.session() as s:
            result = s.run("MATCH (t:E55_Type) RETURN t.name AS name ORDER BY name")
            return [r["name"] for r in result if r["name"]]

    def normalize_type_name(self, name: str, existing: List[str]) -> str:
        """CamelCase normalize and match against existing types."""
        if not name or not name.strip():
            return name
        clean = name.strip()
        lower = clean.lower().replace("_", " ").replace("-", " ")
        for ex in existing:
            if ex.lower().replace("_", " ").replace("-", " ") == lower:
                return ex
        return "".join(w.capitalize() for w in clean.split())

    def resolve_graph_spec(
        self, spec: Dict[str, Any], existing: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """Normalize all type names in a graph spec to match existing types."""
        if existing is None:
            existing = self.get_existing_types()

        nodes = spec.get("nodes", [])
        for node in nodes:
            if not isinstance(node, dict):
                continue
            types = node.get("types", [])
            if isinstance(types, list):
                node["types"] = [
                    self.normalize_type_name(t, existing) for t in types if t
                ]
                existing = list(set(existing) | set(node["types"]))

        return spec
