"""Tests for place-type inference (GraphWriter auto P2 for P7 venues)."""
from __future__ import annotations

import unittest

from pipeline.type_vocab import infer_place_type_name_from_mention


class TestInferPlaceType(unittest.TestCase):
    def test_library_in_name(self) -> None:
        self.assertEqual(infer_place_type_name_from_mention("Victoria Library"), "Library")

    def test_victoria_fallback(self) -> None:
        self.assertEqual(infer_place_type_name_from_mention("Victoria"), "Neighbourhood")


if __name__ == "__main__":
    unittest.main()
