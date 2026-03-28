"""
Unified semantic proposer (v1 contract): live context + Neo4j history fragments → LLM → proposals.

No hard-coded weather thresholds; the model reasons over retrieved E7/E13/E55 subgraph text
and current time/weather JSON. New journal patterns can influence suggestions without code changes.
"""

from __future__ import annotations

import hashlib
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from .weather_open_meteo import geocode_and_forecast

_log = logging.getLogger("uvicorn.error")

# Broad place-name hints so office / shop / school contexts match without hard-coding one city shape.
DEFAULT_PLACE_HINTS = [
    "bureau",
    "office",
    "coworking",
    "atelier",
    "école",
    "school",
    "university",
    "campus",
    "shop",
    "fromagerie",
    "café",
    "cafe",
    "restaurant",
    "gym",
    "tennis",
    "stadium",
    "parc",
    "park",
]


def _self_key(x: str) -> str:
    return (x or "").strip().casefold()


def _local_wall_clock(tz_name: str) -> Tuple[str, str, str]:
    """Returns (local_date YYYY-MM-DD, local iso time string, English weekday name)."""
    try:
        from zoneinfo import ZoneInfo

        z = ZoneInfo((tz_name or "").strip() or "UTC")
    except Exception:
        z = timezone.utc
    now = datetime.now(z)
    wd = ("Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday")[now.weekday()]
    return now.strftime("%Y-%m-%d"), now.isoformat(timespec="seconds"), wd


def _slim_weather(wx: Optional[Dict[str, Any]], days_cap: int = 10) -> Optional[Dict[str, Any]]:
    if not wx:
        return None
    daily = list(wx.get("daily") or [])[:days_cap]
    hourly = list(wx.get("hourly_sample") or [])[:12]
    return {
        "query_city": wx.get("query_city"),
        "location": wx.get("location"),
        "timezone_used": wx.get("timezone_used"),
        "profile_timezone": wx.get("profile_timezone"),
        "current": wx.get("current"),
        "daily": daily,
        "hourly_sample": hourly,
        "attribution": wx.get("attribution"),
    }


def _proposal_id(kind: str, anchor_date: str, title: str) -> str:
    raw = f"{kind}|{anchor_date}|{title}"
    return "pv1-" + hashlib.sha256(raw.encode()).hexdigest()[:16]


def _strip_self_from_people(
    people: List[Dict[str, Any]],
    self_keys: set[str],
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for p in people:
        if not isinstance(p, dict):
            continue
        nm = str(p.get("name") or "").strip()
        if not nm or _self_key(nm) in self_keys:
            continue
        tier = str(p.get("tier") or "neutral").strip() or "neutral"
        out.append({"name": nm, "tier": tier})
    return out


def _finalize_llm_proposal(
    raw: Dict[str, Any],
    *,
    rank: int,
    live_context: Dict[str, Any],
    weather_slim: Optional[Dict[str, Any]],
    self_keys: set[str],
) -> Optional[Dict[str, Any]]:
    kind = str(raw.get("kind") or "suggestion").strip() or "suggestion"
    title = str(raw.get("title") or "").strip()
    body = str(raw.get("body") or "").strip()
    if not title or not body:
        return None
    anchor = str(raw.get("anchor_date") or live_context.get("local_date") or "")[:10]
    if len(anchor) < 10:
        anchor = str(live_context.get("local_date") or datetime.now(timezone.utc).strftime("%Y-%m-%d"))[:10]
    try:
        pr = float(raw.get("priority") or 0.65)
    except (TypeError, ValueError):
        pr = 0.65
    pr = max(0.35, min(0.95, pr)) - rank * 0.015

    people_in = raw.get("people")
    if not isinstance(people_in, list):
        people_in = []
    people = _strip_self_from_people(people_in, self_keys)

    evidence: List[Dict[str, Any]] = []
    if weather_slim:
        evidence.append(
            {
                "type": "weather_context",
                "query_city": weather_slim.get("query_city"),
                "location": weather_slim.get("location"),
            }
        )
    evidence.append(
        {
            "type": "live_context",
            "local_date": live_context.get("local_date"),
            "local_weekday": live_context.get("local_weekday"),
            "profile_city": live_context.get("profile_city"),
        }
    )
    for ge in raw.get("graph_evidence") or []:
        if isinstance(ge, dict) and ge:
            row = {"type": "graph_evidence"}
            row.update(ge)
            evidence.append(row)

    return {
        "id": _proposal_id(kind, anchor, title),
        "kind": kind,
        "title": title,
        "body": body,
        "anchor_date": anchor,
        "valid_from": f"{anchor}T00:00:00",
        "valid_until": f"{anchor}T23:59:59",
        "priority": round(max(0.35, min(0.95, pr)), 3),
        "people": people,
        "evidence": evidence,
    }


def build_proposals_v1(repo: Any, user_name: str, days_ahead: int = 10) -> Dict[str, Any]:
    """
    Semantic proposer returning the same envelope as legacy v1:
      generated_at, proposals[], meta
    """
    days_ahead = max(1, min(int(days_ahead), 14))
    out: Dict[str, Any] = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "proposals": [],
        "meta": {
            "version": 1,
            "engine": "semantic_proposer_v1",
            "kinds_hint": [
                "see_person",
                "visit_place",
                "do_activity",
                "buy_food",
                "suggestion",
            ],
        },
    }

    prof = repo.get_user_profile(user_name=user_name) or {}
    city = (prof.get("current_city") or "").strip()
    tz = (prof.get("timezone") or "").strip()
    local_date, local_time_iso, weekday = _local_wall_clock(tz)

    self_keys = {_self_key(user_name), _self_key(str(prof.get("name") or ""))}
    self_keys.discard("")

    wx: Optional[Dict[str, Any]] = None
    if city:
        try:
            wx = geocode_and_forecast(
                city=city,
                country_hint=(prof.get("home_country") or "").strip() or None,
                timezone_hint=tz or None,
            )
        except Exception as e:
            _log.warning("proposals semantic: weather failed: %s", e)
            out["meta"]["weather_error"] = str(e)
    else:
        out["meta"]["note"] = "no_profile_city_weather_skipped"

    weather_slim = _slim_weather(wx, days_cap=max(days_ahead, 7))

    fragments = repo.semantic_proposal_fragments(
        city_substring=city,
        place_hints=DEFAULT_PLACE_HINTS,
        days=400,
        max_activities=55,
        max_feelings=45,
        entry_cap=100,
    )
    for row in fragments.get("activities") or []:
        if isinstance(row, dict):
            row["actors"] = [
                a
                for a in (row.get("actors") or [])
                if isinstance(a, dict) and _self_key(str(a.get("name") or "")) not in self_keys
            ]

    out["meta"]["fragment_meta"] = fragments.get("meta") or {}
    acts = fragments.get("activities") or []
    feels = fragments.get("feelings") or []
    if not acts and not feels:
        out["meta"]["skipped"] = "no_graph_fragments"
        _log.info("proposals semantic: no fragments user=%s", user_name)
        return out

    live_context: Dict[str, Any] = {
        "user_name": user_name,
        "local_date": local_date,
        "local_time_iso": local_time_iso,
        "local_weekday": weekday,
        "profile_city": city or None,
        "profile_timezone": tz or None,
        "weather": weather_slim,
        "forecast_horizon_days": days_ahead,
    }

    try:
        from .proposal_llm import run_semantic_proposer

        llm_out = run_semantic_proposer(
            live_context,
            fragments,
            user_name=user_name,
            max_proposals=min(5, days_ahead + 2),
        )
    except ValueError as e:
        msg = str(e)
        if "API_KEY" in msg or "not set" in msg.lower():
            out["meta"]["skipped"] = "llm_not_configured"
        else:
            out["meta"]["llm_error"] = msg
        _log.info("proposals semantic: LLM unavailable: %s", msg)
        return out
    except Exception as e:
        _log.warning("proposals semantic: LLM failed: %s", e)
        out["meta"]["llm_error"] = str(e)
        return out

    proposals: List[Dict[str, Any]] = []
    for i, p in enumerate(llm_out.get("proposals") or []):
        if not isinstance(p, dict):
            continue
        fin = _finalize_llm_proposal(
            p,
            rank=i,
            live_context=live_context,
            weather_slim=weather_slim,
            self_keys=self_keys,
        )
        if fin:
            proposals.append(fin)

    proposals.sort(key=lambda x: (x.get("anchor_date") or "", -float(x.get("priority") or 0)))
    out["proposals"] = proposals
    out["meta"]["count"] = len(proposals)
    _log.info("proposals semantic: user=%s proposals=%d", user_name, len(proposals))
    return out
