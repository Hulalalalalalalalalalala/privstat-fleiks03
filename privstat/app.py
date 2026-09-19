"""Public HTTP entry points for the local catalog."""

import csv
import io
import math
import os
import random
import sqlite3
from contextlib import asynccontextmanager, closing
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query, Response
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field, field_validator

from .database import (
    DATASET_ID,
    DEFAULT_EPSILON_BUDGET,
    PUBLIC_FILTER_FIELDS,
    ROOT,
    BudgetExceededError,
    ReleaseConflictError,
    budget_status,
    canonical_filters,
    count_matching_members,
    create_release,
    export_releases,
    initialize_database,
    list_datasets,
    list_releases,
)


class DatasetMetadata(BaseModel):
    id: str
    name: str
    description: str
    synthetic: bool
    fields: list[str]


class ReleaseRequest(BaseModel):
    request_id: str
    dataset_id: str
    filters: dict[str, str] | None = None
    epsilon: float = Field(ge=0.1, le=1.0)

    @field_validator("request_id", "dataset_id")
    @classmethod
    def require_non_blank(cls, value: str) -> str:
        if not isinstance(value, str) or not value.strip():
            raise ValueError("必须是非空字符串")
        return value.strip()

    @field_validator("filters")
    @classmethod
    def only_public_fields(cls, value: dict[str, str] | None) -> dict[str, str] | None:
        if value is None:
            return None
        unsupported = sorted(set(value) - set(PUBLIC_FILTER_FIELDS))
        if unsupported:
            raise ValueError(f"不支持的筛选字段: {', '.join(unsupported)}")
        return value


class ReleaseRecord(BaseModel):
    release_id: str
    request_id: str
    dataset_id: str
    filters: dict[str, str]
    epsilon: float
    published_count: int
    remaining_budget: float
    created_at: str


class PrivacyBudget(BaseModel):
    initial_budget: float
    used_budget: float
    remaining_budget: float


def _laplace_noise(scale: float) -> float:
    # Inverse-CDF Laplace sample; random() is in [0, 1) so the log is finite.
    sample = random.SystemRandom().random() - 0.5
    if sample == 0.0:
        return 0.0
    return -scale * math.copysign(math.log1p(-2.0 * abs(sample)), sample)


def _noisy_count(true_count: int, epsilon: float) -> int:
    noisy = true_count + _laplace_noise(1.0 / epsilon)
    return max(0, math.floor(noisy + 0.5))


def _configured_budget() -> float:
    raw = os.environ.get("PRIVSTAT_EPSILON_BUDGET")
    if raw is None:
        return DEFAULT_EPSILON_BUDGET
    try:
        value = float(raw)
    except ValueError:
        return DEFAULT_EPSILON_BUDGET
    return value if value > 0 else DEFAULT_EPSILON_BUDGET


def _parse_export_time(value: str | None, field_name: str) -> datetime | None:
    """Parse an ISO-8601 export bound into an aware UTC datetime."""
    if value is None:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        raise HTTPException(
            status_code=422,
            detail=f"{field_name} 必须是 ISO-8601 时间（例如 2026-01-01T00:00:00Z）",
        ) from None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


# Fixed, public-only CSV header for partner exports.
EXPORT_CSV_FIELDS = [
    "release_id",
    "request_id",
    "dataset_id",
    "filters",
    "epsilon",
    "published_count",
    "remaining_budget",
    "created_at",
]


def create_app(database_path: Path | None = None) -> FastAPI:
    path = database_path or Path(
        os.environ.get("PRIVSTAT_DATABASE_PATH", ROOT / ".runtime" / "privstat.sqlite3")
    )
    epsilon_budget = _configured_budget()

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

    @application.post("/api/releases", response_model=ReleaseRecord, status_code=201)
    def publish_release(request: ReleaseRequest, response: Response) -> dict:
        if request.dataset_id != DATASET_ID:
            raise HTTPException(status_code=404, detail=f"未知数据集：{request.dataset_id}")
        filters = request.filters or {}
        published_count = _noisy_count(count_matching_members(path, filters), request.epsilon)
        try:
            record, created = create_release(
                path,
                request_id=request.request_id,
                dataset_id=request.dataset_id,
                filters=filters,
                epsilon=request.epsilon,
                published_count=published_count,
                budget=epsilon_budget,
            )
        except ReleaseConflictError:
            raise HTTPException(
                status_code=409, detail="request_id 已用于不同的请求，请更换标识"
            ) from None
        except BudgetExceededError:
            raise HTTPException(
                status_code=409, detail="隐私预算不足，本次发布未记录"
            ) from None
        if not created:
            response.status_code = 200
        return record

    @application.get("/api/privacy-budget", response_model=PrivacyBudget)
    def privacy_budget() -> dict:
        return budget_status(path, epsilon_budget)

    @application.get("/api/releases", response_model=list[ReleaseRecord])
    def releases() -> list[dict]:
        return list_releases(path)

    @application.get("/api/releases/export")
    def export_release_history(
        format: str = Query(default="json"),
        dataset_id: str | None = Query(default=None),
        request_id: str | None = Query(default=None),
        from_: str | None = Query(default=None, alias="from"),
        to: str | None = Query(default=None),
        limit_raw: str = Query(default="50", alias="limit"),
    ):
        # Manual validation keeps every 422 message readable on the partner page.
        if format not in ("json", "csv"):
            raise HTTPException(status_code=422, detail="format 只能是 json 或 csv")
        if dataset_id is not None and dataset_id.strip() != DATASET_ID:
            raise HTTPException(
                status_code=422, detail=f"dataset_id 仅支持 {DATASET_ID}"
            )
        try:
            limit = int(limit_raw)
        except (TypeError, ValueError):
            raise HTTPException(
                status_code=422, detail="limit 必须是 1 到 100 之间的整数"
            ) from None
        if not 1 <= limit <= 100:
            raise HTTPException(
                status_code=422, detail="limit 必须在 1 到 100 之间"
            )
        start = _parse_export_time(from_, "from")
        end = _parse_export_time(to, "to")
        if start is not None and end is not None and start >= end:
            raise HTTPException(status_code=422, detail="from 必须早于 to")
        matched_request_id = request_id.strip() if request_id and request_id.strip() else None
        records = export_releases(
            path,
            dataset_id=dataset_id.strip() if dataset_id is not None else None,
            request_id=matched_request_id,
            start=start,
            end=end,
            limit=limit,
        )
        if format == "json":
            return records
        buffer = io.StringIO()
        writer = csv.writer(buffer)
        writer.writerow(EXPORT_CSV_FIELDS)
        for record in records:
            writer.writerow(
                [
                    record["release_id"],
                    record["request_id"],
                    record["dataset_id"],
                    canonical_filters(record["filters"]),
                    record["epsilon"],
                    record["published_count"],
                    record["remaining_budget"],
                    record["created_at"],
                ]
            )
        return Response(
            content=buffer.getvalue(),
            media_type="text/csv; charset=utf-8",
            headers={
                "Content-Disposition": 'attachment; filename="releases-export.csv"'
            },
        )

    return application


app = create_app()
