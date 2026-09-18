"""Public HTTP entry points for the local catalog and privacy releases."""

import json
import os
import sqlite3
from contextlib import asynccontextmanager, closing
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel

from .database import ROOT, initialize_database, list_datasets
from .privacy import (
    ReleaseError,
    budget_status,
    configured_budget,
    initialize_privacy_store,
    list_releases,
    publish_count,
)


class DatasetMetadata(BaseModel):
    id: str
    name: str
    description: str
    synthetic: bool
    fields: list[str]


def create_app(database_path: Path | None = None) -> FastAPI:
    path = database_path or Path(
        os.environ.get("PRIVSTAT_DATABASE_PATH", ROOT / ".runtime" / "privstat.sqlite3")
    )
    total_budget = configured_budget()

    @asynccontextmanager
    async def lifespan(application: FastAPI):
        initialize_database(path)
        initialize_privacy_store(path, total_budget)
        application.state.database_path = path
        yield

    application = FastAPI(title="PrivStat", lifespan=lifespan, docs_url=None, redoc_url=None)

    @application.get("/", response_class=HTMLResponse)
    def home() -> str:
        return (ROOT / "privstat" / "static" / "index.html").read_text(encoding="utf-8")

    @application.get("/health")
    def health() -> dict[str, str]:
        with closing(sqlite3.connect(path)) as connection:
            connection.execute("SELECT 1").fetchone()
        return {"status": "ok", "service": "privstat"}

    @application.get("/api/datasets", response_model=list[DatasetMetadata])
    def datasets() -> list[dict]:
        return list_datasets(path)

    @application.get("/api/privacy-budget")
    def privacy_budget() -> dict:
        return budget_status(path)

    @application.get("/api/releases")
    def releases() -> list[dict]:
        return list_releases(path)

    @application.post("/api/releases")
    async def create_release(request: Request):
        try:
            payload = await request.json()
        except (json.JSONDecodeError, UnicodeDecodeError):
            return JSONResponse(
                status_code=422,
                content={"error": "请求体必须是合法的 JSON 对象。"},
            )
        # Run in a worker thread: BEGIN IMMEDIATE may block while another
        # publish holds the write lock, and must not stall the event loop.
        try:
            return await run_in_threadpool(publish_count, path, payload)
        except ReleaseError as error:
            return JSONResponse(
                status_code=error.status_code,
                content={"error": error.message},
            )

    return application


app = create_app()
