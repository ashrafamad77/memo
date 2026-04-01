#!/usr/bin/env python3
"""E2E: reset (keep profile), ingest Victoria+Library line, UK hint, print candidates before/after.

Requires Neo4j + Wikidata (network). Run from repo root: python scripts/test_victoria_hint_flow.py
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config import USER_NAME  # noqa: E402
from pipeline.disambiguation_hint import (  # noqa: E402
    refresh_place_candidates_with_user_hint,
    sibling_enrichment_anchor_qid_from_hint,
)
from pipeline.entity_enrichment import enrich_sibling_tasks  # noqa: E402
from pipeline.pipeline import MemoryPipeline  # noqa: E402
from server.neo4j_repo import Neo4jRepo  # noqa: E402

# Avoid "I met Victoria" — the model often tags Victoria as a person, not a place (no inbox task).
JOURNAL_LINE = (
    "We spent the afternoon in Victoria, then browsed the library before going to the station."
)
HINT = "Victoria is the area near London, UK, not the Australian state."


def _pick_place_task(tasks: list, *substrs: str):
    subs = [s.lower() for s in substrs]
    for t in tasks:
        m = str(t.get("mention") or "").lower()
        if any(s in m for s in subs):
            return t
    return None


def _cand_summary(cands: list) -> list[dict]:
    out = []
    for c in (cands or [])[:8]:
        if isinstance(c, dict):
            qid = c.get("qid") or c.get("wikidata_id")
            out.append({"qid": qid, "label": (c.get("label") or "")[:80]})
    return out


def main() -> int:
    pipe = MemoryPipeline()
    repo = Neo4jRepo()
    try:
        return _run(pipe, repo)
    finally:
        try:
            pipe.close()
        except Exception:
            pass
        try:
            repo.close()
        except Exception:
            pass


def _run(pipe: MemoryPipeline, repo: Neo4jRepo) -> int:
    pipe.reset_all(keep_user_profile=True)
    proc = pipe.process_agentic(JOURNAL_LINE)
    entry_id = (proc or {}).get("entry_id") if isinstance(proc, dict) else proc
    if not entry_id:
        print("process_agentic returned no entry_id", file=sys.stderr)
        return 1

    inbox = repo.inbox(limit=50, entry_id=entry_id)
    tasks = [t for t in inbox if str(t.get("type") or "") == "place_wikidata"]
    vic = _pick_place_task(tasks, "victoria")
    lib = _pick_place_task(tasks, "library", "bibliothèque", "bibliotheque")
    if not vic:
        print(
            "No Victoria place_wikidata task; mentions:",
            [t.get("mention") for t in tasks],
            file=sys.stderr,
        )
        return 1
    if not lib:
        print("No library-like place task (library / bibliothèque)", file=sys.stderr)

    vic_id = str(vic["id"])
    lib_id = str(lib["id"]) if lib else ""

    def load_cands(tid: str) -> list:
        row = repo.get_disambiguation_task(tid)
        if not row:
            return []
        c = row.get("candidates")
        if isinstance(c, list):
            return c
        return []

    print("=== entry_id", entry_id)
    print("Victoria before hint:", json.dumps(_cand_summary(load_cands(vic_id)), indent=2))
    if lib_id:
        print("Library before hint:", json.dumps(_cand_summary(load_cands(lib_id)), indent=2))

    un = (USER_NAME or "").strip() or "User"
    profile = repo.get_user_profile(user_name=un) or {}
    cands = refresh_place_candidates_with_user_hint(
        mention=str(vic.get("mention") or "Victoria"),
        entity_label=str(vic.get("entity_label") or "E53_Place"),
        journal_text=JOURNAL_LINE,
        hint=HINT,
        user_profile=profile,
    )
    if not cands:
        print("refresh_place_candidates_with_user_hint returned empty", file=sys.stderr)
        return 1
    repo.update_task_candidates(vic_id, cands)
    anchor = sibling_enrichment_anchor_qid_from_hint(HINT, JOURNAL_LINE)
    if anchor:
        enrich_sibling_tasks(entry_id, vic_id, anchor, "E53_Place", repo)

    print("anchor_qid:", anchor)
    print("Victoria after hint:", json.dumps(_cand_summary(load_cands(vic_id)), indent=2))
    if lib_id:
        print("Library after hint:", json.dumps(_cand_summary(load_cands(lib_id)), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
