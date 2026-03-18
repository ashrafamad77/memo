from __future__ import annotations

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from .neo4j_repo import Neo4jRepo
from pipeline import MemoryPipeline


class ChatIn(BaseModel):
    message: str = Field(..., min_length=1)

class ResolveTaskIn(BaseModel):
    decision: str = Field(..., pattern="^(merge|split)$")
    target_person_id: str | None = None


def create_app() -> FastAPI:
    app = FastAPI(title="Memo UI API", version="0.1.0")
    repo = Neo4jRepo()
    pipeline: MemoryPipeline | None = None

    # Local-first: allow the local Next.js dev server
    app.add_middleware(
        CORSMiddleware,
        # Allow any local dev port (Next.js often runs on 3000/3001/3002...)
        allow_origin_regex=r"^http://(localhost|127\.0\.0\.1)(:\d+)?$",
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

    @app.get("/person/{person_id}")
    def person_detail(person_id: str, entry_limit: int = 30):
        out = repo.person_detail(person_id, entry_limit=entry_limit)
        if not out:
            raise HTTPException(status_code=404, detail="person not found")
        return out

    @app.get("/graph/neighborhood")
    def neighborhood(ref: str, depth: int = 1, limit: int = 200):
        try:
            return repo.neighborhood(ref=ref, depth=depth, limit=limit)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e)) from e

    @app.get("/inbox")
    def inbox(status: str = "open", limit: int = 50):
        return {"items": repo.inbox(status=status, limit=limit)}

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
        try:
            result = pipeline.process_agentic(text)
            return {"type": "add_entry", "result": result}
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e)) from e

    return app


app = create_app()

