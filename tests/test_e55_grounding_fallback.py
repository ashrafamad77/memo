"""Tests for E55 Tier-A grounding fallback (wbsearch + optional embed-related)."""
from __future__ import annotations

import unittest
from unittest.mock import MagicMock, patch


class TestApplyE55TierAFallback(unittest.TestCase):
    def test_skips_when_type_on_denylist(self) -> None:
        from pipeline.e55_grounding_fallback import apply_e55_tier_a_fallback

        row = {"wikidata_id": "", "wikidata_candidates": []}
        out = apply_e55_tier_a_fallback("Visit", "at the library", row)
        self.assertEqual(out.get("wikidata_candidates"), [])
        self.assertFalse(out.get("_e55_fallback_applied"))

    @patch("pipeline.e55_grounding_fallback.MEMO_E55_FALLBACK", True)
    @patch("pipeline.e55_grounding_fallback.MEMO_E55_FALLBACK_DENY_TYPES", "")
    @patch("pipeline.e55_grounding_fallback.llm_expand_e55_queries", return_value=(["pay a visit"], []))
    @patch("pipeline.e55_grounding_fallback._wbsearch_merged")
    @patch("pipeline.type_grounding_embed.resolve_wikidata_from_batch_candidates", return_value=None)
    @patch("pipeline.e55_grounding_fallback._pick_related_by_embed", return_value=None)
    def test_runs_for_visit_when_denylist_empty(
        self,
        _pick: MagicMock,
        _resolve: MagicMock,
        mock_wb: MagicMock,
        _llm: MagicMock,
    ) -> None:
        from pipeline.e55_grounding_fallback import apply_e55_tier_a_fallback

        mock_wb.return_value = [{"qid": "Q9", "label": "Visit", "description": "x"}]
        row = {"wikidata_id": "", "wikidata_candidates": []}
        out = apply_e55_tier_a_fallback("Visit", "we visited", row)
        self.assertTrue(out.get("_e55_fallback_applied"))
        self.assertGreaterEqual(len(out.get("wikidata_candidates") or []), 1)

    @patch("pipeline.e55_grounding_fallback.MEMO_E55_FALLBACK", True)
    @patch("pipeline.e55_grounding_fallback.llm_expand_e55_queries", return_value=(["a"], ["b"]))
    @patch("pipeline.e55_grounding_fallback._wbsearch_merged")
    @patch("pipeline.type_grounding_embed.resolve_wikidata_from_batch_candidates", return_value=None)
    @patch("pipeline.e55_grounding_fallback._pick_related_by_embed", return_value=None)
    def test_fills_candidates_when_wb_returns_hits(
        self,
        _pick: MagicMock,
        _resolve: MagicMock,
        mock_wb: MagicMock,
        _llm: MagicMock,
    ) -> None:
        from pipeline.e55_grounding_fallback import apply_e55_tier_a_fallback

        mock_wb.return_value = [
            {"qid": "Q1", "label": "L1", "description": "D1"},
        ]
        row = {"wikidata_id": "", "wikidata_candidates": [], "confidence": "low"}
        out = apply_e55_tier_a_fallback("ColdApproaching", "journal text", row)
        self.assertTrue(out.get("_e55_fallback_applied"))
        self.assertEqual(len(out.get("wikidata_candidates") or []), 1)

    @patch("pipeline.e55_grounding_fallback.MEMO_E55_FALLBACK", True)
    @patch("pipeline.e55_grounding_fallback.llm_expand_e55_queries", return_value=(["district"], []))
    @patch("pipeline.e55_grounding_fallback._wbsearch_merged")
    @patch("pipeline.type_grounding_embed.resolve_wikidata_from_batch_candidates", return_value=None)
    @patch("pipeline.e55_grounding_fallback._pick_related_by_embed", return_value=None)
    def test_seed_without_qid_neighbourhood_not_blocked(
        self,
        _pick: MagicMock,
        _resolve: MagicMock,
        mock_wb: MagicMock,
        _llm: MagicMock,
    ) -> None:
        from pipeline.e55_grounding_fallback import apply_e55_tier_a_fallback

        mock_wb.return_value = [{"qid": "Q123", "label": "Neighbourhood", "description": "area"}]
        row = {"wikidata_id": "", "wikidata_candidates": []}
        out = apply_e55_tier_a_fallback("Neighbourhood", "walked around the area", row)
        self.assertTrue(out.get("_e55_fallback_applied"))
        self.assertEqual(len(out.get("wikidata_candidates") or []), 1)

    @patch("pipeline.e55_grounding_fallback.MEMO_E55_FALLBACK", True)
    @patch("pipeline.e55_grounding_fallback.llm_expand_e55_queries", return_value=([], []))
    @patch("pipeline.e55_grounding_fallback._wbsearch_merged", return_value=[])
    def test_no_op_when_wb_empty(self, *_mocks: MagicMock) -> None:
        from pipeline.e55_grounding_fallback import apply_e55_tier_a_fallback

        row = {"wikidata_id": "", "wikidata_candidates": []}
        out = apply_e55_tier_a_fallback("XyzzyNovel", "text", row)
        self.assertEqual(out.get("wikidata_candidates"), [])


if __name__ == "__main__":
    unittest.main()
