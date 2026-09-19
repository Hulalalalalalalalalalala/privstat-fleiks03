"""Public HTTP entry points for the local catalog."""

import csv
import io
import json
import math
import os
import random
import secrets
import sqlite3
from contextlib import asynccontextmanager, closing
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query, Response
from fastapi.responses import HTMLResponse, JSONResponse
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
    create_share,
    get_share_by_token,
    initialize_database,
    list_datasets,
    list_releases,
    query_releases,
    revoke_share,
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


class ShareCreateRequest(BaseModel):
    dataset_id: str
    request_id: str | None = None
    from_time: datetime | None = Field(default=None, alias="from")
    to_time: datetime | None = Field(default=None, alias="to")
    limit: int = Field(default=50, ge=1, le=100)
    expires_at: datetime

    @field_validator("dataset_id")
    @classmethod
    def require_non_blank(cls, value: str) -> str:
        if not isinstance(value, str) or not value.strip():
            raise ValueError("必须是非空字符串")
        return value.strip()

    @field_validator("request_id")
    @classmethod
    def blank_request_id_rejected(cls, value: str | None) -> str | None:
        if value is None:
            return None
        if not value.strip():
            raise ValueError("request_id 必须是非空字符串")
        return value.strip()

    @field_validator("from_time", "to_time", "expires_at")
    @classmethod
    def require_timezone(cls, value: datetime | None) -> datetime | None:
        if value is not None and value.tzinfo is None:
            raise ValueError("时间必须携带时区，例如 2026-01-01T00:00:00Z")
        return value


class ShareRecord(BaseModel):
    share_id: str
    token: str
    dataset_id: str
    request_id: str | None
    from_time: datetime | None = Field(serialization_alias="from")
    to_time: datetime | None = Field(serialization_alias="to")
    limit: int
    expires_at: datetime
    created_at: datetime


EXPORT_FIELDS = [
    "release_id",
    "request_id",
    "dataset_id",
    "filters",
    "epsilon",
    "published_count",
    "remaining_budget",
    "created_at",
]


def _parse_export_time(raw: str, name: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        raise HTTPException(
            status_code=422, detail=f"{name} 必须是 ISO-8601 时间，例如 2026-01-01T00:00:00Z"
        ) from None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


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

    @application.post("/api/shares", response_model=ShareRecord, status_code=201)
    def create_partner_share(request: ShareCreateRequest) -> ShareRecord:
        if request.dataset_id != DATASET_ID:
            raise HTTPException(
                status_code=422, detail=f"dataset_id 只能为 {DATASET_ID}"
            )
        start = request.from_time.astimezone(timezone.utc) if request.from_time else None
        end = request.to_time.astimezone(timezone.utc) if request.to_time else None
        expires_at = request.expires_at.astimezone(timezone.utc)
        if start is not None and end is not None and start >= end:
            raise HTTPException(status_code=422, detail="from 必须早于 to")
        now = datetime.now(timezone.utc)
        if expires_at <= now:
            raise HTTPException(status_code=422, detail="expires_at 必须是未来时间")
        # The token is shown once and only its SHA-256 hash is persisted.
        token = secrets.token_urlsafe(32)
        record = create_share(
            path,
            token=token,
            dataset_id=request.dataset_id,
            request_id=request.request_id,
            start=start,
            end=end,
            limit=request.limit,
            expires_at=expires_at,
        )
        return ShareRecord(
            share_id=record["share_id"],
            token=token,
            dataset_id=record["dataset_id"],
            request_id=record["request_id"],
            from_time=datetime.fromisoformat(record["from"]) if record["from"] else None,
            to_time=datetime.fromisoformat(record["to"]) if record["to"] else None,
            limit=record["limit"],
            expires_at=datetime.fromisoformat(record["expires_at"]),
            created_at=datetime.fromisoformat(record["created_at"]),
        )

    @application.get(
        "/api/shares/{token}/releases", response_model=list[ReleaseRecord]
    )
    def share_releases(token: str) -> list[dict]:
        share = get_share_by_token(path, token)
        if share is None:
            raise HTTPException(status_code=404, detail="分享不存在")
        if share["revoked_at"] is not None:
            raise HTTPException(status_code=410, detail="分享已撤销")
        if datetime.now(timezone.utc) >= datetime.fromisoformat(share["expires_at"]):
            raise HTTPException(status_code=410, detail="分享已过期")
        # Read-only: only the releases table is queried, no budget is spent and
        # no record is written; member-level data is never accessed.
        return query_releases(
            path,
            dataset_id=share["dataset_id"],
            request_id=share["request_id"],
            start=datetime.fromisoformat(share["from"]) if share["from"] else None,
            end=datetime.fromisoformat(share["to"]) if share["to"] else None,
            limit=share["limit"],
        )

    @application.delete("/api/shares/{token}", status_code=204)
    def delete_share(token: str) -> Response:
        # Idempotent: revoking a missing or already-revoked share still yields
        # the same committed state and a 204.
        revoke_share(path, token)
        return Response(status_code=204)

    @application.get("/api/releases/export")
    def export_releases(
        format: str = Query(default="json"),
        dataset_id: str | None = Query(default=None),
        request_id: str | None = Query(default=None),
        from_time: str | None = Query(default=None, alias="from"),
        to_time: str | None = Query(default=None, alias="to"),
        limit: int = Query(default=50, ge=1, le=100),
    ) -> Response:
        if format not in ("json", "csv"):
            raise HTTPException(status_code=422, detail="format 只能是 json 或 csv")
        if dataset_id is not None and dataset_id != DATASET_ID:
            raise HTTPException(status_code=422, detail=f"dataset_id 只能为 {DATASET_ID}")
        if request_id is not None:
            if request_id == "":
                # An explicit empty request_id is a valid filter that matches
                # nothing (stored request_ids are non-blank): 200 empty result.
                pass
            elif not request_id.strip():
                raise HTTPException(status_code=422, detail="request_id 必须是非空字符串")
            else:
                request_id = request_id.strip()
        start = _parse_export_time(from_time, "from") if from_time is not None else None
        end = _parse_export_time(to_time, "to") if to_time is not None else None
        if start is not None and end is not None and start >= end:
            raise HTTPException(status_code=422, detail="from 必须早于 to")
        records = query_releases(
            path,
            dataset_id=dataset_id,
            request_id=request_id,
            start=start,
            end=end,
            limit=limit,
        )
        if format == "json":
            return JSONResponse(
                [ReleaseRecord(**record).model_dump() for record in records]
            )
        buffer = io.StringIO()
        writer = csv.writer(buffer)
        writer.writerow(EXPORT_FIELDS)
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
