"""Differentially private count releases backed by a persistent epsilon budget.

Only sanitized aggregate values leave this module: the true count is used to
derive ``published_count`` but is never returned, stored or written into any
error message.
"""

import json
import math
import os
import random
import sqlite3
import uuid
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path

from .database import FIELDS

PUBLIC_FIELDS = tuple(field for field in FIELDS if field != "member_id")
DEFAULT_EPSILON_BUDGET = 3.0
_EPSILON_MIN = 0.1
_EPSILON_MAX = 1.0
# Keep float bookkeeping (0.1 + 0.2 style) away from the budget comparison.
_BUDGET_EPSILON = 1e-9


class ReleaseError(Exception):
    """A release request that must be answered with a specific HTTP status."""

    def __init__(self, status_code: int, message: str):
        super().__init__(message)
        self.status_code = status_code
        self.message = message


def configured_budget() -> float:
    """Read the total epsilon budget from ``PRIVSTAT_EPSILON_BUDGET``."""
    raw = os.environ.get("PRIVSTAT_EPSILON_BUDGET")
    if raw is None or raw.strip() == "":
        return DEFAULT_EPSILON_BUDGET
    try:
        value = float(raw)
    except ValueError:
        value = float("nan")
    if not math.isfinite(value) or value <= 0:
        raise RuntimeError(
            "PRIVSTAT_EPSILON_BUDGET 必须是正数，当前值为 "
            f"{raw!r}。"
        )
    return value


def initialize_privacy_store(path: Path, total_budget: float) -> None:
    """Create release tables and seed the single budget row on first use."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with closing(sqlite3.connect(path)) as connection, connection:
        connection.execute(
            "CREATE TABLE IF NOT EXISTS releases ("
            "release_id TEXT PRIMARY KEY, "
            "request_id TEXT NOT NULL UNIQUE, "
            "request_key TEXT NOT NULL, "
            "request_json TEXT NOT NULL, "
            "published_count INTEGER NOT NULL, "
            "remaining_budget REAL NOT NULL, "
            "created_at TEXT NOT NULL)"
        )
        connection.execute(
            "CREATE TABLE IF NOT EXISTS privacy_budget ("
            "id INTEGER PRIMARY KEY CHECK (id = 1), "
            "total REAL NOT NULL, used REAL NOT NULL)"
        )
        connection.execute(
            "INSERT OR IGNORE INTO privacy_budget (id, total, used) VALUES (1, ?, 0)",
            (total_budget,),
        )


def _validate_payload(payload: object) -> tuple[str, str, list[dict[str, str]], float]:
    if not isinstance(payload, dict):
        raise ReleaseError(422, "请求体必须是 JSON 对象。")

    request_id = payload.get("request_id")
    if not isinstance(request_id, str) or not request_id.strip():
        raise ReleaseError(422, "request_id 必须是非空字符串。")

    dataset_id = payload.get("dataset_id")
    if not isinstance(dataset_id, str) or not dataset_id.strip():
        raise ReleaseError(422, "dataset_id 必须是非空字符串。")

    epsilon = payload.get("epsilon")
    if isinstance(epsilon, bool) or not isinstance(epsilon, (int, float)):
        raise ReleaseError(422, "epsilon 必须是 0.1 到 1.0 之间的数字。")
    epsilon = float(epsilon)
    if not math.isfinite(epsilon) or not (_EPSILON_MIN <= epsilon <= _EPSILON_MAX):
        raise ReleaseError(422, "epsilon 必须在 0.1 到 1.0 之间（含边界）。")

    raw_filters = payload.get("filters")
    if raw_filters is None:
        raw_filters = []
    if not isinstance(raw_filters, list):
        raise ReleaseError(422, "filters 必须为空或等值条件列表。")

    filters: list[dict[str, str]] = []
    seen_fields: set[str] = set()
    for clause in raw_filters:
        if not isinstance(clause, dict) or set(clause.keys()) != {"field", "value"}:
            raise ReleaseError(422, "每个筛选条件只能包含 field 与 value。")
        field = clause["field"]
        value = clause["value"]
        if not isinstance(field, str) or not field.strip():
            raise ReleaseError(422, "筛选字段名必须是非空字符串。")
        if field not in PUBLIC_FIELDS:
            raise ReleaseError(
                422,
                f"不允许按字段 {field} 筛选；仅支持：{', '.join(PUBLIC_FIELDS)}。",
            )
        if not isinstance(value, str) or not value.strip():
            raise ReleaseError(422, f"字段 {field} 的等值条件必须是非空字符串。")
        if field in seen_fields:
            raise ReleaseError(422, f"字段 {field} 出现了多个筛选条件。")
        seen_fields.add(field)
        filters.append({"field": field, "value": value})

    return request_id, dataset_id, filters, epsilon


def _canonical_key(dataset_id: str, filters: list[dict[str, str]], epsilon: float) -> str:
    ordered = sorted(filters, key=lambda clause: clause["field"])
    return json.dumps(
        {"dataset_id": dataset_id, "filters": ordered, "epsilon": epsilon},
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def _laplace_noise(scale: float, rng: random.Random) -> float:
    # Inverse-CDF sampling of Laplace(0, scale) from a uniform draw.
    uniform = 1.0 - rng.random()  # map [0, 1) to (0, 1] so log() is finite
    magnitude = -scale * math.log(uniform)
    return magnitude if rng.random() < 0.5 else -magnitude


def _row_to_release(row: sqlite3.Row) -> dict:
    return {
        "release_id": row["release_id"],
        "request": json.loads(row["request_json"]),
        "published_count": row["published_count"],
        "remaining_budget": row["remaining_budget"],
        "created_at": row["created_at"],
    }


def publish_count(
    path: Path,
    payload: object,
    rng: random.Random | None = None,
) -> dict:
    """Validate, charge budget and return a noisy count release atomically."""
    request_id, dataset_id, filters, epsilon = _validate_payload(payload)
    rng = rng or random.Random()
    request_key = _canonical_key(dataset_id, filters, epsilon)
    original_request = {
        "request_id": request_id,
        "dataset_id": dataset_id,
        "filters": filters,
        "epsilon": epsilon,
    }

    with closing(sqlite3.connect(path, timeout=30)) as connection:
        connection.row_factory = sqlite3.Row
        # IMMEDIATE takes the write lock up front so concurrent publishes
        # serialize: the second caller always observes the first one's result.
        connection.execute("BEGIN IMMEDIATE")
        try:
            if connection.execute(
                "SELECT 1 FROM datasets WHERE id = ?", (dataset_id,)
            ).fetchone() is None:
                raise ReleaseError(404, f"未知数据集：{dataset_id}")

            existing = connection.execute(
                "SELECT * FROM releases WHERE request_id = ?", (request_id,)
            ).fetchone()
            if existing is not None:
                if existing["request_key"] != request_key:
                    raise ReleaseError(
                        409,
                        f"request_id {request_id} 已用于其他请求，"
                        "相同编号的请求参数必须完全一致。",
                    )
                return _row_to_release(existing)

            budget = connection.execute(
                "SELECT total, used FROM privacy_budget WHERE id = 1"
            ).fetchone()
            total, used = float(budget["total"]), float(budget["used"])
            if used + epsilon > total + _BUDGET_EPSILON:
                raise ReleaseError(
                    409,
                    f"隐私预算不足：本次需要 {epsilon:g}，剩余 {total - used:g}。",
                )

            if filters:
                where = " AND ".join(f"{clause['field']} = ?" for clause in filters)
                values = [clause["value"] for clause in filters]
                row = connection.execute(
                    f"SELECT COUNT(*) AS true_count FROM retail_members WHERE {where}",
                    values,
                ).fetchone()
            else:
                row = connection.execute(
                    "SELECT COUNT(*) AS true_count FROM retail_members"
                ).fetchone()
            true_count = int(row["true_count"])
            noisy = true_count + _laplace_noise(1.0 / epsilon, rng)
            # 四舍五入（非银行家舍入）后下限为零。
            published_count = max(0, int(math.floor(noisy + 0.5)))

            new_used = round(used + epsilon, 9)
            remaining_budget = round(total - new_used, 9)
            release_id = uuid.uuid4().hex
            created_at = datetime.now(timezone.utc).isoformat(timespec="microseconds").replace(
                "+00:00", "Z"
            )
            connection.execute(
                "INSERT INTO releases ("
                "release_id, request_id, request_key, request_json, "
                "published_count, remaining_budget, created_at"
                ") VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    release_id,
                    request_id,
                    request_key,
                    json.dumps(original_request, ensure_ascii=False),
                    published_count,
                    remaining_budget,
                    created_at,
                ),
            )
            connection.execute(
                "UPDATE privacy_budget SET used = ? WHERE id = 1", (new_used,)
            )
            connection.execute("COMMIT")
        except Exception:
            connection.execute("ROLLBACK")
            raise

    return {
        "release_id": release_id,
        "request": original_request,
        "published_count": published_count,
        "remaining_budget": remaining_budget,
        "created_at": created_at,
    }


def list_releases(path: Path) -> list[dict]:
    """Return past releases newest first; the true count is never present."""
    with closing(sqlite3.connect(path)) as connection:
        connection.row_factory = sqlite3.Row
        rows = connection.execute(
            "SELECT * FROM releases ORDER BY created_at DESC, rowid DESC"
        ).fetchall()
    return [_row_to_release(row) for row in rows]


def budget_status(path: Path) -> dict:
    with closing(sqlite3.connect(path)) as connection:
        row = connection.execute(
            "SELECT total, used FROM privacy_budget WHERE id = 1"
        ).fetchone()
    total, used = float(row[0]), round(float(row[1]), 9)
    return {"initial": total, "used": used, "remaining": round(total - used, 9)}
