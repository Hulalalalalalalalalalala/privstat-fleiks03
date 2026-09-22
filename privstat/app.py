"""Public HTTP entry points for the local catalog."""

import csv
import io
import json
import math
import os
import random
import sqlite3
from contextlib import asynccontextmanager, closing
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query, Response
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)

from .database import (
    DATASET_ID,
    DEFAULT_EPSILON_BUDGET,
    PUBLIC_FILTER_FIELDS,
    ROOT,
    SHARE_ACCESS_OUTCOMES,
    BatchConflictError,
    BudgetExceededError,
    ReleaseConflictError,
    ShareQuotaExhaustedError,
    budget_status,
    canonical_filters,
    claim_served_access,
    count_matching_members,
    create_release,
    create_release_batch,
    create_share,
    get_share_by_token,
    initialize_database,
    list_datasets,
    list_releases,
    list_share_access_events,
    list_shares,
    query_releases,
    record_share_access_event,
    revoke_share,
    revoke_share_by_id,
    rotate_share,
    summarize_share_usage,
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


class BatchReleaseItem(ReleaseRequest):
    """One release inside a batch: the single-release fields plus nothing else."""

    model_config = ConfigDict(extra="forbid")


class BatchReleaseRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    batch_id: str
    requests: list[BatchReleaseItem] = Field(min_length=1, max_length=20)

    @field_validator("batch_id")
    @classmethod
    def batch_id_non_blank(cls, value: str) -> str:
        if not isinstance(value, str) or not value.strip():
            raise ValueError("batch_id 必须是非空字符串")
        return value.strip()

    @model_validator(mode="after")
    def request_ids_unique_within_batch(self) -> "BatchReleaseRequest":
        seen: set[str] = set()
        duplicates: set[str] = set()
        for item in self.requests:
            if item.request_id in seen:
                duplicates.add(item.request_id)
            seen.add(item.request_id)
        if duplicates:
            raise ValueError(
                "批内 request_id 必须唯一，重复项: " + ", ".join(sorted(duplicates))
            )
        return self


class RotateRequest(BaseModel):
    # Tightened contract: rotation_id is the only accepted field, so an
    # extra/typo key is a client error (422) instead of being ignored.
    model_config = ConfigDict(extra="forbid")

    rotation_id: str

    @field_validator("rotation_id")
    @classmethod
    def rotation_id_non_blank(cls, value: str) -> str:
        if not isinstance(value, str) or not value.strip():
            raise ValueError("rotation_id 必须是非空字符串")
        return value.strip()


class ReleaseRecord(BaseModel):
    release_id: str
    request_id: str
    dataset_id: str
    filters: dict[str, str]
    epsilon: float
    published_count: int
    remaining_budget: float
    created_at: str


class BatchReleaseResponse(BaseModel):
    batch_id: str
    releases: list[ReleaseRecord]
    total_epsilon: float
    remaining_budget: float
    created_at: str


class PrivacyBudget(BaseModel):
    initial_budget: float
    used_budget: float
    remaining_budget: float


class ShareAccessEventRecord(BaseModel):
    # The audit trail exposes these six fields and nothing else: no raw
    # token and no token digest ever leaves the server.
    event_id: str
    share_id: str
    token_version: int
    outcome: str
    result_count: int
    accessed_at: str


class ShareUsageRecord(BaseModel):
    # Management rollup: identity/status plus lifetime quota fields and
    # the windowed aggregates. No token and no token digest is present.
    share_id: str
    dataset_id: str
    status: str
    max_accesses: int | None
    served_count: int
    remaining_accesses: int | None
    event_count: int
    result_count: int
    outcome_counts: dict[str, int]
    last_accessed_at: str | None


def _parse_share_time(raw: object, name: str) -> datetime:
    if not isinstance(raw, str):
        raise ValueError(f"{name} 必须是 ISO-8601 时间字符串，例如 2026-01-01T00:00:00Z")
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        raise ValueError(f"{name} 必须是 ISO-8601 时间，例如 2026-01-01T00:00:00Z") from None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"{name} 必须包含时区，例如 2026-01-01T00:00:00Z")
    return parsed.astimezone(timezone.utc)


class ShareRequest(BaseModel):
    model_config = {"populate_by_name": True}

    dataset_id: str
    request_id: str | None = None
    from_time: datetime | None = Field(default=None, alias="from")
    to_time: datetime | None = Field(default=None, alias="to")
    limit: int = Field(default=50, ge=1, le=100)
    expires_at: datetime
    # Successful-visit quota: omitted or null means unlimited; a supplied
    # value must be an integer in 1-1000.
    max_accesses: int | None = Field(default=None, ge=1, le=1000)

    @field_validator("max_accesses", mode="before")
    @classmethod
    def max_accesses_is_int_or_null(cls, value: object) -> object:
        if value is None:
            return None
        # bool is an int subclass; JSON booleans and numeric strings/floats
        # are rejected rather than silently coerced.
        if not isinstance(value, int) or isinstance(value, bool):
            raise ValueError("max_accesses 必须是 1–1000 的整数或 null")
        return value

    @field_validator("dataset_id")
    @classmethod
    def dataset_non_blank(cls, value: str) -> str:
        if not isinstance(value, str) or not value.strip():
            raise ValueError("dataset_id 必须是非空字符串")
        return value.strip()

    @field_validator("request_id")
    @classmethod
    def request_id_non_blank(cls, value: str | None) -> str | None:
        if value is None:
            return None
        if not isinstance(value, str) or not value.strip():
            raise ValueError("request_id 必须是非空字符串")
        return value.strip()

    @field_validator("from_time", "to_time", "expires_at", mode="before")
    @classmethod
    def parse_time_with_timezone(cls, value: object, info) -> datetime | None:
        if value is None:
            return None
        name = "from" if info.field_name == "from_time" else (
            "to" if info.field_name == "to_time" else "expires_at"
        )
        return _parse_share_time(value, name)


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


def _parse_access_event_time(raw: str, name: str) -> datetime:
    # Audit windows always require an explicit timezone: unlike exports, a
    # bare timestamp is ambiguous and rejected rather than assumed UTC.
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        raise HTTPException(
            status_code=422,
            detail=f"{name} 必须是带时区的 ISO-8601 时间，例如 2026-01-01T00:00:00Z",
        ) from None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise HTTPException(
            status_code=422,
            detail=f"{name} 必须包含时区，例如 2026-01-01T00:00:00Z",
        )
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

    @application.post(
        "/api/releases/batch", response_model=BatchReleaseResponse, status_code=201
    )
    def publish_release_batch(request: BatchReleaseRequest, response: Response) -> dict:
        # Every sub-item targets the one supported dataset: any other
        # dataset_id rejects the whole batch with 404 before any state or
        # budget is touched (the single-release rule, applied wholesale).
        for item in request.requests:
            if item.dataset_id != DATASET_ID:
                raise HTTPException(
                    status_code=404, detail=f"未知数据集：{item.dataset_id}"
                )
        # True counts come from member data, which never leaves this call:
        # only the noisy per-item counts enter the stored records and the
        # response. Counts are gathered before the write transaction so the
        # transaction stays short; no noise is sampled until the batch has
        # confirmed inside the transaction that every identity is free and
        # the total epsilon fits the balance.
        items = [
            {
                "request_id": item.request_id,
                "dataset_id": item.dataset_id,
                "filters": item.filters or {},
                "epsilon": item.epsilon,
                "true_count": count_matching_members(path, item.filters or {}),
            }
            for item in request.requests
        ]
        try:
            header, records, created = create_release_batch(
                path,
                batch_id=request.batch_id,
                items=items,
                budget=epsilon_budget,
                sample_count=_noisy_count,
            )
        except BatchConflictError as error:
            if error.reason == "request_id_occupied":
                raise HTTPException(
                    status_code=409,
                    detail=(
                        f"request_id {error.detail} 已被单条或批量发布占用，"
                        "整批未记录"
                    ),
                ) from None
            raise HTTPException(
                status_code=409,
                detail="batch_id 已用于不同的请求，请更换标识，整批未记录",
            ) from None
        except BudgetExceededError:
            raise HTTPException(
                status_code=409, detail="隐私预算不足，整批发布未记录"
            ) from None
        if not created:
            response.status_code = 200
        return {
            "batch_id": header["batch_id"],
            "releases": records,
            "total_epsilon": header["total_epsilon"],
            "remaining_budget": header["remaining_budget"],
            "created_at": header["created_at"],
        }

    @application.get("/api/privacy-budget", response_model=PrivacyBudget)
    def privacy_budget() -> dict:
        return budget_status(path, epsilon_budget)

    @application.get("/api/releases", response_model=list[ReleaseRecord])
    def releases() -> list[dict]:
        return list_releases(path)

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
        # An explicitly empty request_id= is a valid filter that matches
        # nothing; only whitespace-only values are rejected.
        empty_request_id = request_id == ""
        if request_id is not None and not empty_request_id:
            request_id = request_id.strip()
            if not request_id:
                raise HTTPException(status_code=422, detail="request_id 必须是非空字符串")
        start = _parse_export_time(from_time, "from") if from_time is not None else None
        end = _parse_export_time(to_time, "to") if to_time is not None else None
        if start is not None and end is not None and start >= end:
            raise HTTPException(status_code=422, detail="from 必须早于 to")
        if empty_request_id:
            records = []
        else:
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

    def _share_payload(record: dict) -> dict:
        return {
            "share_id": record["share_id"],
            "token": record["token"],
            "dataset_id": record["dataset_id"],
            "request_id": record["request_id"],
            "from": record["from"],
            "to": record["to"],
            "limit": record["limit"],
            "created_at": record["created_at"],
            "expires_at": record["expires_at"],
            "max_accesses": record["max_accesses"],
            "served_count": record["served_count"],
            "remaining_accesses": (
                None
                if record["max_accesses"] is None
                else max(0, record["max_accesses"] - record["served_count"])
            ),
        }

    @application.post("/api/shares", status_code=201)
    def create_partner_share(request: ShareRequest) -> dict:
        if request.dataset_id != DATASET_ID:
            raise HTTPException(
                status_code=422, detail=f"dataset_id 只能为 {DATASET_ID}"
            )
        if request.expires_at <= datetime.now(timezone.utc):
            raise HTTPException(status_code=422, detail="expires_at 必须在未来")
        if (
            request.from_time is not None
            and request.to_time is not None
            and request.from_time >= request.to_time
        ):
            raise HTTPException(status_code=422, detail="from 必须早于 to")
        record = create_share(
            path,
            dataset_id=request.dataset_id,
            request_id=request.request_id,
            start=request.from_time,
            end=request.to_time,
            limit=request.limit,
            expires_at=request.expires_at,
            max_accesses=request.max_accesses,
        )
        return _share_payload(record)

    @application.get("/api/shares")
    def list_partner_shares(
        status: str | None = Query(default=None),
        dataset_id: str | None = Query(default=None),
        request_id: str | None = Query(default=None),
        limit: int = Query(default=50, ge=1, le=100),
    ) -> list[dict]:
        if status is not None and status not in ("active", "expired", "revoked"):
            raise HTTPException(
                status_code=422,
                detail="status 只能是 active、expired 或 revoked",
            )
        if dataset_id is not None:
            if not dataset_id.strip() or dataset_id != DATASET_ID:
                raise HTTPException(
                    status_code=422, detail=f"未知数据集：{dataset_id}"
                )
        if request_id is not None:
            request_id = request_id.strip()
            if not request_id:
                raise HTTPException(
                    status_code=422, detail="request_id 必须是非空字符串"
                )
        # Management-only listing: shares table is the sole source, so no
        # member data, budget, or release records are touched. Tokens and
        # their digests are never part of the returned records.
        return list_shares(
            path,
            status=status,
            dataset_id=dataset_id,
            request_id=request_id,
            limit=limit,
        )

    @application.get(
        "/api/shares/{token}/releases", response_model=list[ReleaseRecord]
    )
    def shared_releases(token: str) -> list[dict]:
        share = get_share_by_token(path, token)
        if share is None:
            # Unknown tokens are unresolvable, so they are neither audited
            # nor distinguishable from a typo.
            raise HTTPException(status_code=404, detail="未知分享")
        now = datetime.now(timezone.utc)
        # Revocation takes precedence over expiry, which takes precedence
        # over a superseded token generation, which precedes the quota
        # check: none of these rejections spends quota.
        if share["revoked_at"] is not None:
            outcome = "revoked"
        elif datetime.fromisoformat(share["expires_at"]) <= now:
            outcome = "expired"
        elif not share["token_current"]:
            outcome = "superseded"
        else:
            outcome = None
        if outcome is not None:
            # The rejection is audited before the response is produced: if
            # the audit write fails, the error propagates and no share data
            # goes out. A rejection never carries release rows.
            record_share_access_event(
                path,
                share_id=share["share_id"],
                token_version=share["token_version"],
                outcome=outcome,
                result_count=0,
            )
            detail = {
                "revoked": "分享已撤销",
                "expired": "分享已过期",
                "superseded": "分享凭证已轮换，旧令牌已失效",
            }[outcome]
            raise HTTPException(status_code=410, detail=detail)
        # Read-only view over the releases table: no budget is deducted, no
        # release record is created, and member-level data is never read.
        records = query_releases(
            path,
            dataset_id=share["dataset_id"],
            request_id=share["request_id"],
            start=datetime.fromisoformat(share["from"]) if share["from"] else None,
            end=datetime.fromisoformat(share["to"]) if share["to"] else None,
            limit=share["limit"],
        )
        # Counter increment and the served/exhausted audit commit in one
        # SQLite transaction. Rows are returned only on a committed success:
        # exhaustion commits the quota_exhausted event and answers 429 with
        # no data; any other failure rolls both writes back (no quota
        # spent) and answers 500.
        try:
            claim_served_access(
                path,
                share_id=share["share_id"],
                token_version=share["token_version"],
                result_count=len(records),
            )
        except ShareQuotaExhaustedError:
            raise HTTPException(
                status_code=429, detail="分享访问配额已用尽"
            ) from None
        except Exception:
            raise HTTPException(
                status_code=500, detail="访问配额或审计写入失败"
            ) from None
        return records

    @application.get(
        "/api/share-access-events", response_model=list[ShareAccessEventRecord]
    )
    def share_access_events(
        share_id: str | None = Query(default=None),
        outcome: str | None = Query(default=None),
        from_time: str | None = Query(default=None, alias="from"),
        to_time: str | None = Query(default=None, alias="to"),
        limit: int = Query(default=50, ge=1, le=100),
    ) -> list[dict]:
        # Validation mirrors the export endpoint: bad outcome/time/limit or
        # a non-half-open window is a 422 instead of an empty result.
        if outcome is not None and outcome not in SHARE_ACCESS_OUTCOMES:
            raise HTTPException(
                status_code=422,
                detail="outcome 只能是 served、expired、revoked、superseded 或 quota_exhausted",
            )
        if share_id is not None:
            share_id = share_id.strip()
            if not share_id:
                raise HTTPException(
                    status_code=422, detail="share_id 必须是非空字符串"
                )
        start = _parse_access_event_time(from_time, "from") if from_time is not None else None
        end = _parse_access_event_time(to_time, "to") if to_time is not None else None
        if start is not None and end is not None and start >= end:
            raise HTTPException(status_code=422, detail="from 必须早于 to")
        # Querying the audit trail is itself read-only and never audited.
        return list_share_access_events(
            path,
            share_id=share_id,
            outcome=outcome,
            start=start,
            end=end,
            limit=limit,
        )

    @application.get(
        "/api/share-usage", response_model=list[ShareUsageRecord]
    )
    def share_usage(
        share_id: str | None = Query(default=None),
        from_time: str | None = Query(default=None, alias="from"),
        to_time: str | None = Query(default=None, alias="to"),
        limit: int = Query(default=50, ge=1, le=100),
    ) -> list[dict]:
        # Validation mirrors the audit endpoint: a blank identifier, a
        # timezone-less/invalid time, a non-half-open window, or an
        # out-of-range limit is a 422 rather than an empty summary.
        if share_id is not None:
            share_id = share_id.strip()
            if not share_id:
                raise HTTPException(
                    status_code=422, detail="share_id 必须是非空字符串"
                )
        start = (
            _parse_access_event_time(from_time, "from")
            if from_time is not None
            else None
        )
        end = (
            _parse_access_event_time(to_time, "to")
            if to_time is not None
            else None
        )
        if start is not None and end is not None and start >= end:
            raise HTTPException(status_code=422, detail="from 必须早于 to")
        # One read-only transaction reads both tables, so the live quota
        # counters and the windowed event aggregates are mutually
        # consistent even while partners hit shares. Nothing is written
        # and no member, budget, release or credential data is read; an
        # unknown share_id is an empty 200, not an error.
        return summarize_share_usage(
            path,
            share_id=share_id,
            start=start,
            end=end,
            limit=limit,
        )

    @application.post("/api/shares/id/{share_id}/rotate", status_code=201)
    def rotate_partner_share(share_id: str, request: RotateRequest, response: Response) -> dict:
        # A committed rotation_id replays before any revoked/expired
        # check, so retries of a successful rotation always return 200
        # with the original metadata (and never the token again).
        status, record = rotate_share(
            path, share_id=share_id, rotation_id=request.rotation_id
        )
        if status == "not_found":
            raise HTTPException(status_code=404, detail="未知分享")
        if status == "inactive":
            raise HTTPException(
                status_code=409, detail="分享已撤销或已过期，无法轮换凭证"
            )
        if status == "replayed":
            response.status_code = 200
        return record

    @application.delete("/api/shares/id/{share_id}", status_code=204)
    def revoke_partner_share_by_id(share_id: str) -> Response:
        if not revoke_share_by_id(path, share_id):
            raise HTTPException(status_code=404, detail="未知分享")
        return Response(status_code=204)

    @application.delete("/api/shares/{token}", status_code=204)
    def revoke_partner_share(token: str) -> Response:
        # Revocation by token is unconditionally idempotent: an unknown
        # token yields the same 204 as an existing one, so callers cannot
        # probe which tokens exist.
        revoke_share(path, token)
        return Response(status_code=204)

    return application


app = create_app()
