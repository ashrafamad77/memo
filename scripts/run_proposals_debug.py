#!/usr/bin/env python3
"""
Debug entrypoint for AI suggestions (proposals_v1) without HTTP.

Usage: run from VS Code launch "Python: AI suggestions (proposals_v1 direct)"
Set breakpoints in server/proposals_v1.py, server/proposal_llm.py, etc.

Equivalent HTTP: GET http://127.0.0.1:8000/proposals?days_ahead=10 (with API running).
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv

load_dotenv(ROOT / ".env")

from config import USER_NAME  # noqa: E402
from server.neo4j_repo import Neo4jRepo  # noqa: E402
from server.proposals_v1 import build_proposals_v1  # noqa: E402


def main() -> None:
    p = argparse.ArgumentParser(description="Run build_proposals_v1 under debugger")
    p.add_argument("--days-ahead", type=int, default=10, dest="days_ahead", help="Forward window for proposals")
    args = p.parse_args()
    user_name = (USER_NAME or "").strip() or "User"
    repo = Neo4jRepo()
    out = build_proposals_v1(repo, user_name=user_name, days_ahead=args.days_ahead)
    print(json.dumps(out, indent=2, default=str))


if __name__ == "__main__":
    main()
