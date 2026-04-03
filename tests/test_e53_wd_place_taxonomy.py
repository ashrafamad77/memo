"""E53 Wikidata place taxonomy merge."""
from __future__ import annotations

import unittest
from unittest.mock import patch


class TestMergedTaxonomy(unittest.TestCase):
    def test_merged_includes_patched_extra(self) -> None:
        from pipeline.e53_wd_place_taxonomy import DEFAULT_E53_WD_PLACE_CHECKS, merged_e53_wd_place_checks

        with patch(
            "pipeline.e53_wd_place_taxonomy._parse_extra_taxonomy",
            return_value=[("Q99999", "CustomGeo")],
        ):
            m = merged_e53_wd_place_checks()
        self.assertEqual(len(m), len(DEFAULT_E53_WD_PLACE_CHECKS) + 1)
        self.assertEqual(m[-1], ("Q99999", "CustomGeo"))

    def test_merged_without_extra_matches_default(self) -> None:
        from pipeline.e53_wd_place_taxonomy import DEFAULT_E53_WD_PLACE_CHECKS, merged_e53_wd_place_checks

        with patch("pipeline.e53_wd_place_taxonomy._parse_extra_taxonomy", return_value=[]):
            m = merged_e53_wd_place_checks()
        self.assertEqual(m, DEFAULT_E53_WD_PLACE_CHECKS)


if __name__ == "__main__":
    unittest.main()
