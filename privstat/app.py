"""Public HTTP entry points for the local catalog."""

import os
import sqlite3
from contextlib import asynccontextmanager, closing
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

from .database import ROOT, initialize_database, list_datasets


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

    @asynccontextmanager
    async def lifespan(application: FastAPI):
        initialize_database(path)
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

    return application


app = create_app()
