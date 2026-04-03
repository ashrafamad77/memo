from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from datetime import datetime

import json

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
from typing import Iterator, Optional

from .neo4j_repo import Neo4jRepo
from pipeline import MemoryPipeline
from pipeline.extractor import ExtractedEntity
from config import USER_NAME, CORS_ORIGINS

_log = logging.getLogger("uvicorn.error")


@asynccontextmanager
async def _app_lifespan(app: FastAPI):
    """Log embedding sidecar config (vectors load in Docker, not in this process)."""
    try:
        from config import EMBEDDING_INFERENCE_URL
        from pipeline.embedding_service import embedding_dim

        _log.info(
            "Embeddings via HTTP %s (dim=%s); ensure t2v-transformers is running (e.g. docker compose up).",
            EMBEDDING_INFERENCE_URL,
            embedding_dim(),
        )
    except Exception as e:
        _log.warning("Embedding config log failed: %s", e)
    yield


class ChatIn(BaseModel):
    message: str = Field(..., min_length=1)
    """When set, ``message`` is treated as a free-text hint to refine Wikidata options for this open task."""
    disambiguation_hint_task_id: Optional[str] = None

class ResolveTaskIn(BaseModel):
    decision: str = Field(..., pattern="^(merge|split|pick|skip)$")
    target_person_id: str | None = None
    wikidata_id: str | None = None


class ProfileUpsertIn(BaseModel):
    """Same fields as chat onboarding — set any subset (local dev / skipping UI questionnaire)."""

    current_city: str | None = None
    home_country: str | None = None
    nationality: str | None = None
    timezone: str | None = None
    work_context: str | None = None


ONBOARDING_STEPS = [
    ("current_city", "Before we start, where are you physically based most of the time (city)?"),
    ("home_country", "What is your home country?"),
    ("nationality", "What is your nationality? (or type 'skip')"),
    ("timezone", "What is your main timezone? (example: Europe/Paris)"),
    ("work_context", "Briefly describe your recurring work context (example: online lectures for An-Najah in Nablus)."),
]


def _needs_onboarding(profile: dict, entry_count: int) -> bool:
    required = ["current_city", "home_country", "timezone"]
    missing = [k for k in required if not (profile.get(k) or "").strip()]
    return bool(missing) and entry_count == 0


def _is_user_asking_clarification(text: str) -> bool:
    t = (text or "").strip().lower()
    if not t:
        return False
    question_markers = [
        "?",
        "what do you mean",
        "what does",
        "what does that mean",
        "what that means",
        "what this means",
        "what is meant",
        "que veux-tu dire",
        "what means",
        "c'est quoi",
        "ça veut dire",
        "which one",
        "can you explain",
        "explain this",
    ]
    if any(m in t for m in question_markers):
        return True
    # Generic fallback for "what ... mean(s)" phrasings.
    if t.startswith("what") and (" mean" in t or "means" in t):
        return True
    return False


def _onboarding_field_help(field: str) -> str:
    if field == "current_city":
        return "Current city means where you physically live/work most of the time right now (example: Paris)."
    if field == "home_country":
        return (
            "Home country means your main country of origin/background used for context in your notes. "
            "It does not have to be birthplace. If you have multiple, give the primary one."
        )
    if field == "nationality":
        return "Nationality means your legal citizenship (you can provide one or more). You can also type 'skip'."
    if field == "timezone":
        return "Timezone means the local time reference you usually use (example: Europe/Paris)."
    if field == "work_context":
        return "Work context means your recurring professional setup (example: online lectures to An-Najah while based in France)."
    return "Please provide a short value for this profile field."


def _looks_like_journal_entry(text: str) -> bool:
    t = (text or "").strip()
    if len(t) > 120:
        return True
    words = t.split()
    if len(words) > 22:
        return True
    t_l = t.lower()
    narrative_markers = [
        "i woke up",
        "i had",
        "i started",
        "today",
        "this morning",
        "suddenly",
        "and then",
    ]
    return any(m in t_l for m in narrative_markers)


def _validate_onboarding_answer(field: str, text: str) -> tuple[bool, str, str]:
    val = (text or "").strip()
    if not val:
        return False, "Please provide a value.", ""
    if _is_user_asking_clarification(val):
        return False, "I detected a clarification question, not a profile value.", val
    if field != "work_context" and _looks_like_journal_entry(val):
        return False, "This looks like a journal entry; I need a short profile value.", val

    if field in {"current_city", "home_country"}:
        if len(val) < 2 or len(val) > 50:
            return False, "Please provide a short city/country name.", val
        if len(val.split()) > 6:
            return False, "Please keep this short (one city/country label).", val
        return True, "", val

    if field == "nationality":
        if val.lower() == "skip":
            return True, "", ""
        if len(val) > 80:
            return False, "Please provide only nationality/citizenship info (short).", val
        return True, "", val

    if field == "timezone":
        tz = val.replace(" ", "")
        looks_tz = ("/" in tz) or tz.upper().startswith("UTC") or tz.upper().startswith("GMT")
        if not looks_tz and len(val.split()) > 3:
            return False, "Please provide a timezone like Europe/Paris or UTC+1.", val
        return True, "", val

    if field == "work_context":
        if len(val) < 8:
            return False, "Please provide a bit more detail for work context.", val
        if len(val) > 260:
            return False, "Please keep work context concise (max ~1-2 lines).", val
        return True, "", val

    return True, "", val


def _detect_location_ambiguity(text: str, extraction, profile: dict) -> dict | None:
    text_l = (text or "").lower()
    current_city = (profile.get("current_city") or "").strip()
    if not current_city:
        return None

    remote_markers = ["online", "en ligne", "à distance", "remote", "virtuel", "zoom", "teams"]
    lecture_markers = ["lecture", "cours", "university", "université", "najah", "an-najah"]
    has_remote_context = any(m in text_l for m in remote_markers) or any(m in text_l for m in lecture_markers)
    if not has_remote_context:
        return None

    places = []
    for e in extraction.entities:
        if e.label == "Place" and (e.text or "").strip():
            places.append(e.text.strip())
    places_norm = {p.lower(): p for p in places}
    if not places_norm:
        return None

    # Ambiguous if text contains non-current-city places in a likely remote context.
    non_local = [v for k, v in places_norm.items() if k != current_city.lower()]
    if not non_local:
        return None

    remote_place = non_local[0]
    question = (
        f"I detected place '{remote_place}', but your profile says you're based in {current_city}. "
        f"Was this event physically in {current_city} and only remote/context related to {remote_place}? "
        "Reply 'yes' (physical local, remote context) or 'no' (I was physically there)."
    )
    return {"question": question, "current_city": current_city, "remote_place": remote_place, "non_local_places": non_local}


def _apply_location_clarification(
    extraction,
    current_city: str,
    remote_place: str,
    is_remote_context: bool,
    non_local_places: list[str] | None = None,
):
    entities = list(extraction.entities)
    if is_remote_context:
        # v2: keep all extracted Place mentions as Place nodes.
        # Storage decides whether place is physical occurrence vs contextual reference.
        # using the metadata clarified_* overrides.
        has_current_city_place = False
        for e in entities:
            if e.label == "Place" and (e.text or "").strip().lower() == current_city.strip().lower():
                has_current_city_place = True
                break
        if not has_current_city_place and current_city.strip():
            entities.append(ExtractedEntity(text=current_city.strip(), label="Place", start_char=0, end_char=0))
        extraction.entities = entities
        meta = extraction.metadata or {}
        meta["clarified_physical_place"] = current_city.strip()
        meta["clarified_remote_context_place"] = remote_place.strip()
        if non_local_places:
            meta["clarified_remote_context_places"] = non_local_places
        extraction.metadata = meta
    return extraction


def create_app() -> FastAPI:
    app = FastAPI(title="Memo UI API", version="0.1.0", lifespan=_app_lifespan)
    repo = Neo4jRepo()
    pipeline: MemoryPipeline | None = None
    chat_state: dict = {
        "mode": None,  # None | onboarding | clarifying | disambiguating
        "onboarding_step": 0,
        "onboarding_answers": {},
        "pending": None,       # for clarifier payload
        "pending_disambig": None,  # for LLM disambiguation flow
    }

    def _add_entry_bundle(result: dict) -> dict:
        eid = str(result.get("entry_id") or "").strip()
        open_tasks = repo.inbox(status="open", limit=50, entry_id=eid) if eid else []
        return {"type": "add_entry", "result": result, "open_tasks": open_tasks}

    def _sse_line(obj: dict) -> str:
        return f"data: {json.dumps(obj, ensure_ascii=False)}\n\n"

    def _disambig_question_text(item: dict, current: int, total: int) -> str:
        name = item.get("name", "")
        cidoc = item.get("cidoc_label", "")
        reason = (item.get("reason") or "").strip()
        candidates: list = item.get("candidates") or []
        suggestion = (item.get("canonical_label") or "").strip()
        # Don't show suggestion if it's the same as the surface form (no work done)
        if suggestion.lower() == name.lower():
            suggestion = ""
        type_hint = {
            "E53_Place": "place",
            "E21_Person": "person",
            "E74_Group": "organisation",
        }.get(cidoc, cidoc or "entity")
        lines = [f'({current}/{total}) What do you mean by "{name}"?  ({type_hint})']
        if reason:
            lines.append(f"Note: {reason}")
        if candidates:
            lines.append("\nTop candidates:")
            for i, c in enumerate(candidates[:3], 1):
                lines.append(f"  {i}. {c}")
            if suggestion and suggestion not in candidates:
                lines.append(f"\nSystem suggestion: \"{suggestion}\"")
            lines.append('\nUse the buttons in the chat panel if available, or type a candidate name, or "skip".')
        elif suggestion:
            # ChatPanel already shows "Use suggestion: …" — avoid duplicating it in the prose.
            lines.append(
                "Use the green suggestion button in the panel, type a different name below, or \"skip\"."
            )
        else:
            lines.append('\nType the canonical name (e.g. "Victoria, London") or "skip" to leave as-is.')
        return "\n".join(lines)

    def _clarification_ui_payload(item: dict) -> dict:
        """Structured payload for chat UIs (inline picks). Text-only clients can ignore it."""
        name = str(item.get("name") or "")
        cands = [str(c).strip() for c in (item.get("candidates") or []) if str(c).strip()][:3]
        sug = str(item.get("canonical_label") or "").strip()
        if sug.lower() == name.lower():
            sug = ""
        return {
            "id": str(item.get("id") or ""),
            "name": name,
            "candidates": cands,
            "suggestion": sug or None,
        }

    def _pick_from_candidates(user_text: str, candidates: list) -> str:
        """If user typed '1', '2', or '3', return the corresponding candidate; else return user_text."""
        t = user_text.strip()
        if t in ("1", "2", "3") and candidates:
            idx = int(t) - 1
            if idx < len(candidates):
                return candidates[idx]
        return t

    # CORS: allow localhost (dev) and VPS
    origins = [o.strip() for o in CORS_ORIGINS.split(",") if o.strip()]
    app.add_middleware(
        CORSMiddleware,
        allow_origins=origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @app.get("/health")
    def health():
        return {"ok": True}

    @app.get("/timeline")
    def timeline(limit: int = 50):
        return {"items": repo.timeline(limit=limit)}

    @app.get("/entry/{entry_id}")
    def entry_detail(entry_id: str):
        out = repo.entry_detail(entry_id)
        if not out:
            raise HTTPException(status_code=404, detail="entry not found")
        return out

    @app.delete("/entry/{entry_id}")
    def delete_entry(entry_id: str):
        """Delete journal entry from Neo4j (scoped nodes + E73) and from Weaviate when available."""
        eid = (entry_id or "").strip()
        if not eid:
            raise HTTPException(status_code=400, detail="entry_id is required")
        graph_out = repo.delete_journal_entry(eid)
        if not graph_out.get("ok"):
            raise HTTPException(status_code=404, detail=graph_out.get("reason") or "entry not found")
        vector_ok = False
        try:
            from pipeline.vector_store import VectorStore

            vs = VectorStore()
            vector_ok = vs.delete_by_entry_id(eid)
        except Exception:
            vector_ok = False
        return {"ok": True, "entry_id": eid, "vector_deleted": vector_ok}

    @app.get("/persons")
    def persons(query: str = "", limit: int = 50):
        return {"items": repo.persons(query=query, limit=limit)}

    @app.get("/entities")
    def entities(query: str = "", limit: int = 120, category: str = ""):
        return {"items": repo.entities(query=query, limit=limit, category=category)}

    @app.get("/briefing/activity-focus")
    def briefing_activity_focus(hours: int = 24):
        """Recent graph activity types for the Basic / daily briefing tab (no LLM)."""
        return repo.briefing_activity_focus(hours=hours)

    @app.get("/briefing/world-context")
    def briefing_world_context(limit: int = 5):
        """Headlines: separate city and home-country feeds (Google News RSS), up to `limit` each."""
        from .news_context import fetch_profile_news_split

        user_name = (USER_NAME or "").strip() or "User"
        prof = repo.get_user_profile(user_name=user_name) or {}
        return fetch_profile_news_split(prof, per_section=limit)

    @app.get("/profile")
    def get_profile():
        """Onboarding / user context (Neo4j User node). Used by Basic tab and APIs."""
        user_name = (USER_NAME or "").strip() or "User"
        row = repo.get_user_profile(user_name=user_name) or {}

        def _s(key: str, default: str = "") -> str:
            v = row.get(key)
            if v is None:
                return default
            return str(v).strip()

        return {
            "name": _s("name") or user_name,
            "current_city": _s("current_city"),
            "home_country": _s("home_country"),
            "nationality": _s("nationality"),
            "timezone": _s("timezone"),
            "work_context": _s("work_context"),
        }

    @app.post("/profile")
    def upsert_profile(body: ProfileUpsertIn):
        """Upsert journal owner profile in Neo4j (mirrors chat onboarding save)."""
        user_name = (USER_NAME or "").strip() or "User"
        raw = body.model_dump(exclude_none=True)
        fields = {k: str(v).strip() for k, v in raw.items() if str(v or "").strip()}
        if not fields:
            raise HTTPException(
                status_code=400,
                detail="Provide at least one non-empty field (e.g. current_city, home_country, timezone).",
            )
        return repo.upsert_user_profile(user_name=user_name, fields=fields)

    @app.get("/weather")
    def get_weather():
        """Current + short forecast for profile `current_city` via Open-Meteo (no API key)."""
        user_name = (USER_NAME or "").strip() or "User"
        prof = repo.get_user_profile(user_name=user_name) or {}
        city = (prof.get("current_city") or "").strip()
        if not city:
            return {
                "ok": False,
                "code": "no_city",
                "message": "Your profile has no current city yet. Complete onboarding in the chat when asked where you are based most of the time.",
            }
        try:
            from .weather_open_meteo import geocode_and_forecast

            data = geocode_and_forecast(
                city=city,
                timezone_hint=(prof.get("timezone") or "").strip() or None,
            )
            return {"ok": True, **data}
        except RuntimeError as e:
            return {"ok": False, "code": "weather_error", "message": str(e)}
        except Exception as e:
            _log.exception("weather upstream failure")
            raise HTTPException(status_code=502, detail="Weather service error") from e

    @app.get("/entity/nav-options")
    def entity_nav_options(ref: str, anchor_person: str = ""):
        if not (ref or "").strip():
            raise HTTPException(status_code=400, detail="ref is required")
        try:
            return repo.entity_navigation_options(ref=ref.strip(), anchor_person=(anchor_person or "").strip())
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e)) from e

    @app.get("/entity/overview")
    def entity_overview(ref: str, limit: int = 120, anchor_person: str = "", focus: str = ""):
        return repo.entity_overview(
            ref=ref,
            limit=limit,
            anchor_person=(anchor_person or "").strip(),
            focus=(focus or "").strip(),
        )

    @app.get("/person/{person_id}")
    def person_detail(person_id: str, entry_limit: int = 30):
        out = repo.person_detail(person_id, entry_limit=entry_limit)
        if not out:
            raise HTTPException(status_code=404, detail="person not found")
        return out

    @app.get("/person/{person_id}/timeline")
    def person_timeline(person_id: str, limit: int = 100):
        return {"items": repo.person_timeline(person_id=person_id, limit=limit)}

    @app.get("/graph/neighborhood")
    def neighborhood(ref: str, depth: int = 1, limit: int = 200):
        try:
            return repo.neighborhood(ref=ref, depth=depth, limit=limit)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e)) from e

    @app.get("/inbox")
    def inbox(status: str = "open", limit: int = 50, entry_id: str = ""):
        eid = (entry_id or "").strip()
        return {
            "items": repo.inbox(
                status=status, limit=limit, entry_id=eid if eid else None
            )
        }

    @app.get("/proposals")
    def proposals(days_ahead: int = 10):
        """Context-aware suggestions (v1: nice-weather × supportive / emerging people)."""
        user_name = (USER_NAME or "").strip() or "User"
        from .proposals_v1 import build_proposals_v1

        return build_proposals_v1(repo, user_name=user_name, days_ahead=days_ahead)

    @app.get("/insights")
    def insights(days: int = 30):
        user_name = (USER_NAME or "").strip() or "User"
        return repo.insights(user_name=user_name, days=days)

    @app.get("/insights/person")
    def insights_person(person: str, days: int = 30, limit: int = 40):
        person_name = (person or "").strip()
        if not person_name:
            raise HTTPException(status_code=400, detail="person is required")
        return repo.insights_person_detail(person_name=person_name, days=days, limit=limit)

    @app.post("/inbox/{task_id}/resolve")
    def resolve_task(task_id: str, payload: ResolveTaskIn):
        try:
            result = repo.resolve_task(
                task_id=task_id,
                decision=payload.decision,
                target_person_id=payload.target_person_id,
                wikidata_id=payload.wikidata_id,
            )
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e)) from e

        # Run sibling re-enrichment before returning so the client's immediate
        # inbox refresh sees updated candidates (BackgroundTasks run after the
        # response is sent, which races with refresh() in the UI).
        enrich_meta = result.pop("_enrich", None)
        if (
            enrich_meta
            and enrich_meta.get("entry_id")
            and enrich_meta.get("resolved_qid")
            and enrich_meta.get("entity_label") == "E53_Place"
        ):
            from pipeline.entity_enrichment import enrich_sibling_tasks

            enrich_sibling_tasks(
                enrich_meta["entry_id"],
                task_id,
                enrich_meta["resolved_qid"],
                enrich_meta["entity_label"],
                repo,
            )

        return result

    @app.post("/chat")
    def chat(_in: ChatIn):
        nonlocal pipeline
        text = _in.message.strip()
        if not text:
            raise HTTPException(status_code=400, detail="empty message")
        if pipeline is None:
            pipeline = MemoryPipeline(use_graph=True, use_vector=True)

        user_name = (USER_NAME or "").strip() or "User"

        hint_tid = (_in.disambiguation_hint_task_id or "").strip()
        if hint_tid:
            row = repo.get_disambiguation_task(hint_tid)
            if not row:
                raise HTTPException(status_code=404, detail="disambiguation task not found")
            if str(row.get("status") or "") != "open":
                raise HTTPException(status_code=400, detail="task is already resolved")
            if str(row.get("type") or "") != "place_wikidata":
                raise HTTPException(
                    status_code=400,
                    detail="free-text hints are only supported for place Wikidata tasks",
                )
            eid = str(row.get("entry_id") or "").strip()
            if not eid:
                raise HTTPException(status_code=400, detail="task has no entry_id")
            detail = repo.entry_detail(eid)
            journal = str((detail or {}).get("text") or "")
            profile = repo.get_user_profile(user_name=user_name) or {}
            from pipeline.disambiguation_hint import (
                refresh_place_candidates_with_user_hint,
                sibling_enrichment_anchor_qid_from_hint,
            )

            cands = refresh_place_candidates_with_user_hint(
                mention=str(row.get("mention") or ""),
                entity_label=str(row.get("entity_label") or "E53_Place"),
                journal_text=journal,
                hint=text,
                user_profile=profile,
            )
            if not cands:
                return {
                    "type": "disambiguation_hint",
                    "ok": False,
                    "message": "I could not find new Wikidata matches from that hint. Try naming a city, country, or landmark (e.g. “central London”, “BC Canada”).",
                    "task_id": hint_tid,
                    "entry_id": eid,
                    "mention": str(row.get("mention") or ""),
                    "candidates": [],
                }
            repo.update_task_candidates(hint_tid, cands)
            anchor_q = sibling_enrichment_anchor_qid_from_hint(text, journal)
            if anchor_q:
                from pipeline.entity_enrichment import enrich_sibling_tasks

                enrich_sibling_tasks(
                    eid,
                    hint_tid,
                    anchor_q,
                    "E53_Place",
                    repo,
                )
            return {
                "type": "disambiguation_hint",
                "ok": True,
                "message": f'Updated options for “{row.get("mention") or "this place"}” using your note.',
                "task_id": hint_tid,
                "entry_id": eid,
                "mention": str(row.get("mention") or ""),
                "candidates": cands,
            }

        # 1) Continue onboarding flow if in progress.
        if chat_state["mode"] == "onboarding":
            idx = int(chat_state.get("onboarding_step", 0))
            if idx >= len(ONBOARDING_STEPS):
                chat_state["mode"] = None
            else:
                field, _ = ONBOARDING_STEPS[idx]
                # If the user asks what the field means, answer and keep same step.
                if _is_user_asking_clarification(text):
                    help_text = _onboarding_field_help(field)
                    _, q = ONBOARDING_STEPS[idx]
                    return {
                        "type": "question",
                        "mode": "onboarding",
                        "question": f"{help_text}\n\n{q}",
                    }
                val = text.strip()
                ok, err, norm_val = _validate_onboarding_answer(field, val)
                if not ok:
                    _, q = ONBOARDING_STEPS[idx]
                    return {
                        "type": "question",
                        "mode": "onboarding",
                        "question": f"{err}\n\n{q}",
                    }
                val = norm_val
                chat_state["onboarding_answers"][field] = val
                idx += 1
                chat_state["onboarding_step"] = idx
                if idx < len(ONBOARDING_STEPS):
                    _, q = ONBOARDING_STEPS[idx]
                    return {"type": "question", "mode": "onboarding", "question": q}

                profile = repo.upsert_user_profile(user_name=user_name, fields=chat_state["onboarding_answers"])
                chat_state["mode"] = None
                chat_state["onboarding_step"] = 0
                chat_state["onboarding_answers"] = {}
                return {
                    "type": "profile_saved",
                    "message": "Profile saved. You can now send journal entries.",
                    "profile": profile,
                }

        # 2a) Continue LLM disambiguation flow — collect answers one at a time.
        if chat_state["mode"] == "disambiguating" and isinstance(chat_state.get("pending_disambig"), dict):
            pd = chat_state["pending_disambig"]
            clarifications: list = pd["clarifications"]
            idx: int = pd["current_idx"]
            answers: dict = pd["answers"]
            journal_text_pd: str = pd["text"]

            # Record answer for current item
            current_item = clarifications[idx]
            user_raw = text.strip()
            if user_raw.lower() == "skip":
                resolved_label = current_item["name"]
            else:
                resolved_label = _pick_from_candidates(user_raw, current_item.get("candidates") or [])
            answers[current_item["id"]] = resolved_label
            idx += 1
            pd["current_idx"] = idx

            # ── Context propagation ──────────────────────────────────────────────
            # Build context_hints from all answers so far (surface name → resolved label).
            # Re-run disambiguation for remaining items: some may now be auto-resolvable.
            if idx < len(clarifications):
                context_hints = {
                    clarifications[i]["name"]: answers[clarifications[i]["id"]]
                    for i in range(idx)
                    if clarifications[i]["id"] in answers
                }
                remaining = clarifications[idx:]
                try:
                    from pipeline.llm_disambiguator import resolve_remaining_with_context
                    refreshed = resolve_remaining_with_context(
                        journal_text_pd, remaining, context_hints
                    )
                except Exception:
                    refreshed = remaining

                # Auto-accept items the LLM resolved via context
                auto_accepted = [r for r in refreshed if not r.get("needs_clarification")]
                still_pending = [r for r in refreshed if r.get("needs_clarification")]

                for r in auto_accepted:
                    answers[r["id"]] = r.get("canonical_label") or r["name"]

                if still_pending:
                    # Replace remaining clarifications with the refreshed pending list
                    pd["clarifications"] = clarifications[:idx] + still_pending
                    pd["current_idx"] = idx
                    next_item = still_pending[0]
                    total_remaining = len(still_pending)
                    return {
                        "type": "question",
                        "mode": "clarification",
                        "question": _disambig_question_text(next_item, 1, total_remaining),
                        "clarification": _clarification_ui_payload(next_item),
                    }
                # All remaining resolved via context — fall through to pipeline run

            # All answered (or resolved via context) — run the full pipeline
            chat_state["mode"] = None
            chat_state["pending_disambig"] = None
            try:
                result = pipeline.process_agentic(
                    text=journal_text_pd,
                    clarification_answers=answers,
                )
                return _add_entry_bundle(result)
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e)) from e

        # 2b) Continue clarifier flow if waiting for one answer.
        if chat_state["mode"] == "clarifying" and isinstance(chat_state.get("pending"), dict):
            pending = chat_state["pending"]
            ans = text.strip().lower()
            is_yes = ans in {"yes", "y", "oui", "o", "true"}
            is_no = ans in {"no", "n", "non", "false"}
            if not (is_yes or is_no):
                return {
                    "type": "question",
                    "question": "Please reply with 'yes' or 'no' so I can store this correctly.",
                }
            extraction = pending["extraction"]
            original_text = pending["text"]
            if is_yes:
                extraction = _apply_location_clarification(
                    extraction=extraction,
                    current_city=pending["current_city"],
                    remote_place=pending["remote_place"],
                    is_remote_context=True,
                        non_local_places=pending.get("non_local_places", []),
                )
            result = pipeline.process_agentic(text=original_text)
            chat_state["mode"] = None
            chat_state["pending"] = None
            return _add_entry_bundle(result)

        # 3) New user onboarding trigger (cold start).
        profile = repo.get_user_profile(user_name=user_name)
        entry_count = repo.entry_count()
        if _needs_onboarding(profile=profile, entry_count=entry_count):
            chat_state["mode"] = "onboarding"
            chat_state["onboarding_step"] = 0
            chat_state["onboarding_answers"] = {}
            _, q = ONBOARDING_STEPS[0]
            return {
                "type": "question",
                "mode": "onboarding",
                "question": "Quick onboarding (5 short questions) to improve accuracy.\n" + q,
            }

        try:
            # 4) LLM disambiguation pre-flight: ask about ambiguous mentions before storing.
            clarifications = pipeline.get_disambiguation_questions(text)
            if clarifications:
                chat_state["mode"] = "disambiguating"
                chat_state["pending_disambig"] = {
                    "text": text,
                    "clarifications": clarifications,
                    "answers": {},
                    "current_idx": 0,
                }
                first = clarifications[0]
                return {
                    "type": "question",
                    "mode": "clarification",
                    "question": (
                        f"Before I store this entry, I need to clarify "
                        f"{len(clarifications)} mention{'s' if len(clarifications) > 1 else ''}.\n\n"
                        + _disambig_question_text(first, 1, len(clarifications))
                    ),
                    "clarification": _clarification_ui_payload(first),
                }

            # 5) Clarifier pre-check: extract first, ask one follow-up if ambiguous.
            extraction = pipeline.extractor.extract(text)
            extraction = pipeline._resolve_relative_dates(extraction, input_dt=datetime.now())
            ambiguity = _detect_location_ambiguity(text=text, extraction=extraction, profile=profile)
            if ambiguity:
                chat_state["mode"] = "clarifying"
                chat_state["pending"] = {
                    "text": text,
                    "extraction": extraction,
                    "current_city": ambiguity["current_city"],
                    "remote_place": ambiguity["remote_place"],
                    "non_local_places": ambiguity.get("non_local_places", []),
                }
                return {"type": "question", "mode": "clarification", "question": ambiguity["question"]}

            result = pipeline.process_agentic(text=text)
            return _add_entry_bundle(result)
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e)) from e

    @app.post("/chat/stream")
    def chat_stream(_in: ChatIn):
        """Same routing as ``/chat``, but streams agentic progress via Server-Sent Events.

        Events:
        - ``{"type": "stage", "stage", "label", "pct", "detail", "preview"}`` — pipeline checkpoint
        - ``{"type": "done", "payload": ...}`` — same JSON shape as a normal ``/chat`` response
        - ``{"type": "error", "detail": "..."}`` — pipeline failure (HTTP 200; client should fall back)
        """
        nonlocal pipeline
        text = _in.message.strip()
        if not text:
            raise HTTPException(status_code=400, detail="empty message")
        if pipeline is None:
            pipeline = MemoryPipeline(use_graph=True, use_vector=True)

        user_name = (USER_NAME or "").strip() or "User"

        hint_tid = (_in.disambiguation_hint_task_id or "").strip()
        if hint_tid:
            row = repo.get_disambiguation_task(hint_tid)
            if not row:
                raise HTTPException(status_code=404, detail="disambiguation task not found")
            if str(row.get("status") or "") != "open":
                raise HTTPException(status_code=400, detail="task is already resolved")
            if str(row.get("type") or "") != "place_wikidata":
                raise HTTPException(
                    status_code=400,
                    detail="free-text hints are only supported for place Wikidata tasks",
                )
            eid = str(row.get("entry_id") or "").strip()
            if not eid:
                raise HTTPException(status_code=400, detail="task has no entry_id")
            detail = repo.entry_detail(eid)
            journal = str((detail or {}).get("text") or "")
            profile = repo.get_user_profile(user_name=user_name) or {}
            from pipeline.disambiguation_hint import (
                refresh_place_candidates_with_user_hint,
                sibling_enrichment_anchor_qid_from_hint,
            )

            cands = refresh_place_candidates_with_user_hint(
                mention=str(row.get("mention") or ""),
                entity_label=str(row.get("entity_label") or "E53_Place"),
                journal_text=journal,
                hint=text,
                user_profile=profile,
            )
            if not cands:

                def _hint_fail():
                    yield _sse_line(
                        {
                            "type": "done",
                            "payload": {
                                "type": "disambiguation_hint",
                                "ok": False,
                                "message": "I could not find new Wikidata matches from that hint. Try naming a city, country, or landmark (e.g. “central London”, “BC Canada”).",
                                "task_id": hint_tid,
                                "entry_id": eid,
                                "mention": str(row.get("mention") or ""),
                                "candidates": [],
                            },
                        }
                    )

                return StreamingResponse(_hint_fail(), media_type="text/event-stream")
            repo.update_task_candidates(hint_tid, cands)
            anchor_q = sibling_enrichment_anchor_qid_from_hint(text, journal)
            if anchor_q:
                from pipeline.entity_enrichment import enrich_sibling_tasks

                enrich_sibling_tasks(
                    eid,
                    hint_tid,
                    anchor_q,
                    "E53_Place",
                    repo,
                )

            def _hint_ok():
                yield _sse_line(
                    {
                        "type": "done",
                        "payload": {
                            "type": "disambiguation_hint",
                            "ok": True,
                            "message": f'Updated options for “{row.get("mention") or "this place"}” using your note.',
                            "task_id": hint_tid,
                            "entry_id": eid,
                            "mention": str(row.get("mention") or ""),
                            "candidates": cands,
                        },
                    }
                )

            return StreamingResponse(_hint_ok(), media_type="text/event-stream")

        def gen() -> Iterator[str]:
            # 1) Continue onboarding flow if in progress.
            if chat_state["mode"] == "onboarding":
                idx = int(chat_state.get("onboarding_step", 0))
                if idx >= len(ONBOARDING_STEPS):
                    chat_state["mode"] = None
                else:
                    field, _ = ONBOARDING_STEPS[idx]
                    if _is_user_asking_clarification(text):
                        help_text = _onboarding_field_help(field)
                        _, q = ONBOARDING_STEPS[idx]
                        yield _sse_line(
                            {
                                "type": "done",
                                "payload": {
                                    "type": "question",
                                    "mode": "onboarding",
                                    "question": f"{help_text}\n\n{q}",
                                },
                            }
                        )
                        return
                    val = text.strip()
                    ok, err, norm_val = _validate_onboarding_answer(field, val)
                    if not ok:
                        _, q = ONBOARDING_STEPS[idx]
                        yield _sse_line(
                            {
                                "type": "done",
                                "payload": {
                                    "type": "question",
                                    "mode": "onboarding",
                                    "question": f"{err}\n\n{q}",
                                },
                            }
                        )
                        return
                    val = norm_val
                    chat_state["onboarding_answers"][field] = val
                    idx += 1
                    chat_state["onboarding_step"] = idx
                    if idx < len(ONBOARDING_STEPS):
                        _, q = ONBOARDING_STEPS[idx]
                        yield _sse_line(
                            {
                                "type": "done",
                                "payload": {"type": "question", "mode": "onboarding", "question": q},
                            }
                        )
                        return

                    profile = repo.upsert_user_profile(user_name=user_name, fields=chat_state["onboarding_answers"])
                    chat_state["mode"] = None
                    chat_state["onboarding_step"] = 0
                    chat_state["onboarding_answers"] = {}
                    yield _sse_line(
                        {
                            "type": "done",
                            "payload": {
                                "type": "profile_saved",
                                "message": "Profile saved. You can now send journal entries.",
                                "profile": profile,
                            },
                        }
                    )
                    return

            # 2a) Continue LLM disambiguation flow — collect answers one at a time.
            if chat_state["mode"] == "disambiguating" and isinstance(chat_state.get("pending_disambig"), dict):
                pd = chat_state["pending_disambig"]
                clarifications: list = pd["clarifications"]
                idx: int = pd["current_idx"]
                answers: dict = pd["answers"]
                journal_text_pd: str = pd["text"]

                current_item = clarifications[idx]
                user_raw = text.strip()
                if user_raw.lower() == "skip":
                    resolved_label = current_item["name"]
                else:
                    resolved_label = _pick_from_candidates(user_raw, current_item.get("candidates") or [])
                answers[current_item["id"]] = resolved_label
                idx += 1
                pd["current_idx"] = idx

                if idx < len(clarifications):
                    context_hints = {
                        clarifications[i]["name"]: answers[clarifications[i]["id"]]
                        for i in range(idx)
                        if clarifications[i]["id"] in answers
                    }
                    remaining = clarifications[idx:]
                    try:
                        from pipeline.llm_disambiguator import resolve_remaining_with_context

                        refreshed = resolve_remaining_with_context(
                            journal_text_pd, remaining, context_hints
                        )
                    except Exception:
                        refreshed = remaining

                    auto_accepted = [r for r in refreshed if not r.get("needs_clarification")]
                    still_pending = [r for r in refreshed if r.get("needs_clarification")]

                    for r in auto_accepted:
                        answers[r["id"]] = r.get("canonical_label") or r["name"]

                    if still_pending:
                        pd["clarifications"] = clarifications[:idx] + still_pending
                        pd["current_idx"] = idx
                        next_item = still_pending[0]
                        total_remaining = len(still_pending)
                        yield _sse_line(
                            {
                                "type": "done",
                                "payload": {
                                    "type": "question",
                                    "mode": "clarification",
                                    "question": _disambig_question_text(next_item, 1, total_remaining),
                                    "clarification": _clarification_ui_payload(next_item),
                                },
                            }
                        )
                        return

                chat_state["mode"] = None
                chat_state["pending_disambig"] = None
                try:
                    for ev in pipeline.iter_process_agentic(
                        text=journal_text_pd,
                        clarification_answers=answers,
                    ):
                        if ev["type"] == "stage":
                            yield _sse_line(ev)
                        elif ev["type"] == "complete":
                            yield _sse_line({"type": "done", "payload": _add_entry_bundle(ev["result"])})
                except Exception as e:
                    yield _sse_line({"type": "error", "detail": str(e)})
                return

            # 2b) Continue clarifier flow if waiting for one answer.
            if chat_state["mode"] == "clarifying" and isinstance(chat_state.get("pending"), dict):
                pending = chat_state["pending"]
                ans = text.strip().lower()
                is_yes = ans in {"yes", "y", "oui", "o", "true"}
                is_no = ans in {"no", "n", "non", "false"}
                if not (is_yes or is_no):
                    yield _sse_line(
                        {
                            "type": "done",
                            "payload": {
                                "type": "question",
                                "question": "Please reply with 'yes' or 'no' so I can store this correctly.",
                            },
                        }
                    )
                    return
                extraction = pending["extraction"]
                original_text = pending["text"]
                if is_yes:
                    extraction = _apply_location_clarification(
                        extraction=extraction,
                        current_city=pending["current_city"],
                        remote_place=pending["remote_place"],
                        is_remote_context=True,
                        non_local_places=pending.get("non_local_places", []),
                    )
                try:
                    for ev in pipeline.iter_process_agentic(text=original_text):
                        if ev["type"] == "stage":
                            yield _sse_line(ev)
                        elif ev["type"] == "complete":
                            yield _sse_line({"type": "done", "payload": _add_entry_bundle(ev["result"])})
                    chat_state["mode"] = None
                    chat_state["pending"] = None
                except Exception as e:
                    yield _sse_line({"type": "error", "detail": str(e)})
                return

            # 3) New user onboarding trigger (cold start).
            profile = repo.get_user_profile(user_name=user_name)
            entry_count = repo.entry_count()
            if _needs_onboarding(profile=profile, entry_count=entry_count):
                chat_state["mode"] = "onboarding"
                chat_state["onboarding_step"] = 0
                chat_state["onboarding_answers"] = {}
                _, q = ONBOARDING_STEPS[0]
                yield _sse_line(
                    {
                        "type": "done",
                        "payload": {
                            "type": "question",
                            "mode": "onboarding",
                            "question": "Quick onboarding (5 short questions) to improve accuracy.\n" + q,
                        },
                    }
                )
                return

            try:
                clarifications = pipeline.get_disambiguation_questions(text)
                if clarifications:
                    chat_state["mode"] = "disambiguating"
                    chat_state["pending_disambig"] = {
                        "text": text,
                        "clarifications": clarifications,
                        "answers": {},
                        "current_idx": 0,
                    }
                    first = clarifications[0]
                    yield _sse_line(
                        {
                            "type": "done",
                            "payload": {
                                "type": "question",
                                "mode": "clarification",
                                "question": (
                                    f"Before I store this entry, I need to clarify "
                                    f"{len(clarifications)} mention{'s' if len(clarifications) > 1 else ''}.\n\n"
                                    + _disambig_question_text(first, 1, len(clarifications))
                                ),
                                "clarification": _clarification_ui_payload(first),
                            },
                        }
                    )
                    return

                extraction = pipeline.extractor.extract(text)
                extraction = pipeline._resolve_relative_dates(extraction, input_dt=datetime.now())
                ambiguity = _detect_location_ambiguity(text=text, extraction=extraction, profile=profile)
                if ambiguity:
                    chat_state["mode"] = "clarifying"
                    chat_state["pending"] = {
                        "text": text,
                        "extraction": extraction,
                        "current_city": ambiguity["current_city"],
                        "remote_place": ambiguity["remote_place"],
                        "non_local_places": ambiguity.get("non_local_places", []),
                    }
                    yield _sse_line(
                        {
                            "type": "done",
                            "payload": {
                                "type": "question",
                                "mode": "clarification",
                                "question": ambiguity["question"],
                            },
                        }
                    )
                    return

                for ev in pipeline.iter_process_agentic(text=text):
                    if ev["type"] == "stage":
                        yield _sse_line(ev)
                    elif ev["type"] == "complete":
                        yield _sse_line({"type": "done", "payload": _add_entry_bundle(ev["result"])})
            except Exception as e:
                yield _sse_line({"type": "error", "detail": str(e)})

        return StreamingResponse(gen(), media_type="text/event-stream")

    return app


app = create_app()

