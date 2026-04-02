"""Tests for Wikidata Vector client, verify gate, and BabelNet QID pivot."""
from __future__ import annotations

import unittest
from typing import Any
from unittest.mock import MagicMock, patch

try:
    import httpx  # noqa: F401

    from pipeline.wd_vector_verify import is_clear_vector_winner, pick_wikidata_qid_from_hits
    from pipeline.wikidata_vector_client import search_items
    from pipeline import babelnet_client as bn
except ImportError:
    httpx = None  # type: ignore[misc, assignment]
    is_clear_vector_winner = pick_wikidata_qid_from_hits = None  # type: ignore[misc, assignment]
    search_items = None  # type: ignore[misc, assignment]
    bn = None  # type: ignore[misc, assignment]

_DEPS_OK = httpx is not None and search_items is not None


class TestLookupByLabelContextual(unittest.TestCase):
    @patch(
        "pipeline.embedding_service.embed_text",
        side_effect=RuntimeError("no embed in test"),
    )
    @patch("pipeline.babelnet_client.enrich_babel_synset")
    @patch("pipeline.babelnet_client.get_senses")
    def test_lexical_prefers_gloss_aligned_with_type_and_journal(
        self,
        mock_senses: MagicMock,
        mock_enrich: MagicMock,
        _mock_embed: MagicMock,
    ) -> None:
        from pipeline.babelnet_client import lookup_by_label_contextual

        mock_senses.return_value = [
            {"synsetID": {"id": "bn:sojourn"}},
            {"synsetID": {"id": "bn:social_visit"}},
        ]

        def enrich_side(sid: str, **kwargs: Any) -> dict:
            if "sojourn" in sid:
                return {
                    "wikidata_qids": [],
                    "wordnet_ids": ["sojourn%1:04:00::"],
                    "gloss": "sojourn",
                    "wikipedia_en_keys": [],
                    "wiki_other": [],
                }
            return {
                "wikidata_qids": [],
                "wordnet_ids": ["visit%1:04:03::"],
                "gloss": "pay a visit",
                "wikipedia_en_keys": [],
                "wiki_other": [],
            }

        mock_enrich.side_effect = enrich_side

        out = lookup_by_label_contextual(
            "visit",
            api_key="k",
            journal_text="I spent the morning at Victoria then coded at the library",
            type_label="Visit",
            max_candidates=4,
        )
        self.assertEqual(out["synset_id"], "bn:social_visit")


@unittest.skipUnless(_DEPS_OK, "Install project requirements (httpx) for vector grounding tests")
class TestWikidataVectorClient(unittest.TestCase):
    @patch("pipeline.wikidata_vector_client.httpx.Client")
    def test_search_items_normalizes_qid(self, mock_cls: MagicMock) -> None:
        assert search_items is not None
        mock_resp = MagicMock()
        mock_resp.json.return_value = [
            {"QID": "Q42", "similarity_score": 0.9, "rrf_score": 0.04, "reranker_score": 0.88},
            {"QID": "bad", "similarity_score": 0.5},
        ]
        mock_resp.raise_for_status = MagicMock()
        mock_inst = MagicMock()
        mock_inst.__enter__ = MagicMock(return_value=mock_inst)
        mock_inst.__exit__ = MagicMock(return_value=False)
        mock_inst.get.return_value = mock_resp
        mock_cls.return_value = mock_inst

        out = search_items(
            "Douglas Adams",
            base_url="https://wd-vectordb.wmcloud.org",
            api_secret="secret",
            k=5,
            lang="en",
            rerank=True,
        )
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0]["qid"], "Q42")
        self.assertAlmostEqual(out[0]["reranker_score"], 0.88)
        call_kw = mock_inst.get.call_args
        self.assertIn("/item/query/", call_kw[0][0])
        headers = call_kw[1]["headers"]
        self.assertEqual(headers.get("X-API-SECRET"), "secret")
        req_params = call_kw[1].get("params") or {}
        self.assertNotIn("instanceof", req_params)

    @patch("pipeline.wikidata_vector_client.httpx.Client")
    def test_search_items_sends_instanceof_when_set(self, mock_cls: MagicMock) -> None:
        assert search_items is not None
        mock_resp = MagicMock()
        mock_resp.json.return_value = []
        mock_resp.raise_for_status = MagicMock()
        mock_inst = MagicMock()
        mock_inst.__enter__ = MagicMock(return_value=mock_inst)
        mock_inst.__exit__ = MagicMock(return_value=False)
        mock_inst.get.return_value = mock_resp
        mock_cls.return_value = mock_inst
        search_items(
            "test",
            base_url="https://wd-vectordb.wmcloud.org",
            api_secret="x",
            instance_of="Q5,Q234",
        )
        req_params = mock_inst.get.call_args[1]["params"]
        self.assertEqual(req_params.get("instanceof"), "Q5,Q234")


@unittest.skipUnless(_DEPS_OK, "Install project requirements (httpx) for vector grounding tests")
class TestWdVectorVerify(unittest.TestCase):
    def test_clear_winner_reranker_margin(self) -> None:
        assert is_clear_vector_winner is not None
        hits = [
            {"qid": "Q1", "reranker_score": 0.9},
            {"qid": "Q2", "reranker_score": 0.2},
        ]
        self.assertTrue(
            is_clear_vector_winner(hits, margin=0.05, min_score=0.0),
        )

    def test_ambiguous_triggers_llm_path(self) -> None:
        assert pick_wikidata_qid_from_hits is not None
        hits = [
            {"qid": "Q1", "reranker_score": 0.51},
            {"qid": "Q2", "reranker_score": 0.50},
        ]

        def fetcher(qids: list) -> dict:
            return {q: (f"L-{q}", f"D-{q}") for q in qids}

        with patch("pipeline.wd_vector_verify.llm_pick_qid", return_value="Q2"):
            q = pick_wikidata_qid_from_hits(
                hits,
                journal_text="We met in Paris.",
                mention_name="Paris",
                canonical_label="Paris",
                margin=0.05,
                min_score=0.0,
                llm_verify_top=3,
                verify_pool_top_n=5,
                label_fetcher=fetcher,
            )
        self.assertEqual(q, "Q2")

    def test_extra_context_bypasses_clear_winner_when_flagged(self) -> None:
        """E53 + resolved co-mentions should not lock in top-1 without LLM verify."""
        assert pick_wikidata_qid_from_hits is not None
        hits = [
            {"qid": "Q1", "reranker_score": 0.9},
            {"qid": "Q2", "reranker_score": 0.1},
        ]

        def fetcher(qids: list) -> dict:
            return {q: (f"L-{q}", f"D-{q}") for q in qids}

        extra = 'Resolved mentions\n- "Victoria" → "Victoria, London"'
        with patch("pipeline.wd_vector_verify.llm_pick_qid", return_value="Q2") as m_llm:
            q = pick_wikidata_qid_from_hits(
                hits,
                journal_text="At Victoria Library.",
                mention_name="Victoria Library",
                canonical_label="Victoria Library",
                margin=0.05,
                min_score=0.0,
                llm_verify_top=3,
                verify_pool_top_n=5,
                label_fetcher=fetcher,
                extra_llm_context=extra,
                skip_clear_winner_if_context=True,
            )
        self.assertEqual(q, "Q2")
        m_llm.assert_called_once()
        self.assertIn("London", m_llm.call_args.kwargs.get("extra_context", ""))


@unittest.skipUnless(_DEPS_OK, "Install project requirements (httpx) for vector grounding tests")
class TestBabelNetWikidataPivot(unittest.TestCase):
    @patch("pipeline.babelnet_client.httpx.Client")
    def test_get_synset_ids_from_wikidata_parses_list(self, mock_cls: MagicMock) -> None:
        assert bn is not None
        mock_resp = MagicMock()
        mock_resp.json.return_value = [
            {"id": "bn:03083790n", "pos": "NOUN", "source": "BABELNET"},
        ]
        mock_resp.raise_for_status = MagicMock()
        mock_inst = MagicMock()
        mock_inst.__enter__ = MagicMock(return_value=mock_inst)
        mock_inst.__exit__ = MagicMock(return_value=False)
        mock_inst.get.return_value = mock_resp
        mock_cls.return_value = mock_inst

        ids = bn.get_synset_ids_from_wikidata("Q4837690", api_key="k")
        self.assertEqual(ids, ["bn:03083790n"])


class TestResolveE53QidFromVectorHits(unittest.TestCase):
    @patch("pipeline.type_resolver.wikidata_qid_eligible_for_e53_entity_linking")
    def test_prefers_first_eligible_in_hit_order_over_bad_pick(
        self, mock_el: MagicMock
    ) -> None:
        from pipeline.type_resolver import resolve_e53_qid_from_vector_hits

        def side(qid: str):
            if qid == "Q_WRONG":
                return False
            if qid == "Q27087104":
                return True
            return False

        mock_el.side_effect = side
        hits = [{"qid": "Q27087104"}, {"qid": "Q1200052"}]
        self.assertEqual(resolve_e53_qid_from_vector_hits("Q_WRONG", hits), "Q27087104")

    @patch("pipeline.type_resolver.wikidata_qid_eligible_for_e53_entity_linking")
    def test_returns_preferred_when_eligible(self, mock_el: MagicMock) -> None:
        from pipeline.type_resolver import resolve_e53_qid_from_vector_hits

        mock_el.return_value = True
        self.assertEqual(resolve_e53_qid_from_vector_hits("Q1", [{"qid": "Q2"}]), "Q1")

    @patch("pipeline.type_resolver.wikidata_qid_eligible_for_e53_entity_linking")
    def test_wdqs_unknown_falls_back_to_first_unknown_in_order(
        self, mock_el: MagicMock
    ) -> None:
        from pipeline.type_resolver import resolve_e53_qid_from_vector_hits

        # Dummy QIDs: must match Q + digits so _safe_wikidata_qid keeps them (not a real WD claim).
        q_rejected = "Q900001"
        q_wdqs_unknown = "Q900002"

        def side(qid: str):
            if qid == q_rejected:
                return False
            if qid == q_wdqs_unknown:
                return None
            return True

        mock_el.side_effect = side
        hits = [{"qid": q_wdqs_unknown}]
        self.assertEqual(
            resolve_e53_qid_from_vector_hits(q_rejected, hits), q_wdqs_unknown
        )


if __name__ == "__main__":
    unittest.main()
