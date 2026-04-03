"""E53 place E55_Type refinement from linked Wikidata QID."""
from __future__ import annotations

import unittest
from unittest.mock import patch


class TestRefineE53PlaceTypes(unittest.TestCase):
    def test_replaces_neighbourhood_when_wikidata_implies_country(self) -> None:
        from pipeline.type_resolver import refine_e53_place_types_from_wikidata

        nodes = [
            {
                "label": "E53_Place",
                "name": "Israel",
                "types": ["Neighbourhood"],
                "properties": {"wikidata_id": "Q801"},
            }
        ]
        with patch(
            "pipeline.type_resolver.e53_place_e55_type_from_wikidata",
            return_value="Country",
        ):
            refine_e53_place_types_from_wikidata(nodes)
        self.assertEqual(nodes[0]["types"], ["Country"])

    def test_leaves_specific_type_untouched(self) -> None:
        from pipeline.type_resolver import refine_e53_place_types_from_wikidata

        nodes = [
            {
                "label": "E53_Place",
                "name": "Victoria Library",
                "types": ["Library"],
                "properties": {"wikidata_id": "Q27087104"},
            }
        ]
        with patch(
            "pipeline.type_resolver.e53_place_e55_type_from_wikidata",
            return_value="Country",
        ):
            refine_e53_place_types_from_wikidata(nodes)
        self.assertEqual(nodes[0]["types"], ["Library"])


if __name__ == "__main__":
    unittest.main()
