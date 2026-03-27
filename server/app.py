from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from datetime import datetime

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from .neo4j_repo import Neo4jRepo
from pipeline import MemoryPipeline
from pipeline.extractor import ExtractedEntity
from config import USER_NAME, CORS_ORIGINS

_log = logging.getLogger("uvicorn.error")


@asynccontextmanager
async def _app_lifespan(app: FastAPI):
    """Preload the shared embedding model once so the first /chat is not a long silent wait."""
    try:
        from pipeline.embedding_service import embedding_dim

        _log.info("Loading embedding model (first time can take ~30s on CPU; see HF / sentence-transformers logs)...")
        embedding_dim()
        _log.info("Embedding model ready.")
    except Exception as e:
        _log.warning("Embedding preload skipped (will load on first use): %s", e)
    yield


class ChatIn(BaseModel):
    message: str = Field(..., min_length=1)

class ResolveTaskIn(BaseModel):
    decision: str = Field(..., pattern="^(merge|split)$")
    target_person_id: str | None = None


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
        "mode": None,  # None | onboarding | clarifying
        "onboarding_step": 0,
        "onboarding_answers": {},
        "pending": None,  # for clarifier payload
    }

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

    @app.get("/persons")
    def persons(query: str = "", limit: int = 50):
        return {"items": repo.persons(query=query, limit=limit)}

    @app.get("/entities")
    def entities(query: str = "", limit: int = 120, category: str = ""):
        return {"items": repo.entities(query=query, limit=limit, category=category)}

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
    def inbox(status: str = "open", limit: int = 50):
        return {"items": repo.inbox(status=status, limit=limit)}

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
            return repo.resolve_task(
                task_id=task_id,
                decision=payload.decision,
                target_person_id=payload.target_person_id,
            )
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e)) from e

    @app.post("/chat")
    def chat(_in: ChatIn):
        nonlocal pipeline
        text = _in.message.strip()
        if not text:
            raise HTTPException(status_code=400, detail="empty message")
        if pipeline is None:
            pipeline = MemoryPipeline(use_graph=True, use_vector=True)

        user_name = (USER_NAME or "").strip() or "User"

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

        # 2) Continue clarifier flow if waiting for one answer.
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
            return {"type": "add_entry", "result": result}

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
            # 4) Clarifier pre-check: extract first, ask one follow-up if ambiguous.
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
            return {"type": "add_entry", "result": result}
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e)) from e

    return app


app = create_app()

