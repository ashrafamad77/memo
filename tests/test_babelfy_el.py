"""Unit tests for Babelfy client and mention-span helpers.

Requires ``httpx`` and ``requests`` (see ``requirements.txt``). If those are not
installed, tests are skipped so ``python -m unittest discover -s tests`` still runs.
"""
from __future__ import annotations

import unittest
import uuid

try:
    import httpx  # noqa: F401
    import requests  # noqa: F401

    from pipeline.babelfy_client import _cache_key, disambiguate
    from pipeline.babelfy_entity_link import _find_mention_span
except ImportError:
    httpx = None  # type: ignore[misc, assignment]
    _cache_key = disambiguate = None  # type: ignore[misc, assignment]
    _find_mention_span = None  # type: ignore[misc, assignment]

from unittest.mock import MagicMock, patch

_DEPS_OK = httpx is not None and disambiguate is not None


@unittest.skipUnless(_DEPS_OK, "Install project requirements (httpx, requests) to run Babelfy tests")
class TestMentionSpan(unittest.TestCase):
    def test_exact_substring(self) -> None:
        assert _find_mention_span is not None
        self.assertEqual(_find_mention_span("Hello Paris world", "Paris"), (6, 11))

    def test_flexible_whitespace(self) -> None:
        assert _find_mention_span is not None
        self.assertEqual(
            _find_mention_span("We went to New  York today", "New York"),
            (10, 20),
        )


@unittest.skipUnless(_DEPS_OK, "Install project requirements (httpx, requests) to run Babelfy tests")
class TestBabelfyClient(unittest.TestCase):
    def test_cache_key_stable(self) -> None:
        assert _cache_key is not None
        a = _cache_key("hello", "EN", "NAMED_ENTITIES", "WIKI", "PARTIAL_MATCHING")
        b = _cache_key("hello", "EN", "NAMED_ENTITIES", "WIKI", "PARTIAL_MATCHING")
        self.assertEqual(a, b)
        c = _cache_key("hellp", "EN", "NAMED_ENTITIES", "WIKI", "PARTIAL_MATCHING")
        self.assertNotEqual(a, c)

    @patch("pipeline.babelfy_client.httpx.Client")
    def test_disambiguate_and_cache(self, mock_cls: MagicMock) -> None:
        assert disambiguate is not None
        mock_resp = MagicMock()
        mock_resp.json.return_value = [
            {"charFragment": {"start": 0, "end": 2}, "DBpediaURL": ""}
        ]
        mock_resp.raise_for_status = MagicMock()
        mock_inst = MagicMock()
        mock_inst.__enter__ = MagicMock(return_value=mock_inst)
        mock_inst.__exit__ = MagicMock(return_value=False)
        mock_inst.get.return_value = mock_resp
        mock_cls.return_value = mock_inst

        text = "t-" + uuid.uuid4().hex
        out1 = disambiguate(text, api_key="k")
        out2 = disambiguate(text, api_key="k")
        self.assertEqual(len(out1), 1)
        self.assertEqual(out1, out2)
        self.assertEqual(mock_inst.get.call_count, 1)

        st: dict = {}
        disambiguate(text, api_key="k", stats=st)
        self.assertTrue(st.get("babelfy_cache_hit"))


if __name__ == "__main__":
    unittest.main()
