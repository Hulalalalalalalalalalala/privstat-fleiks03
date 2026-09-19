"""SQLite storage for the bundled synthetic data catalog."""

import csv
import json
import sqlite3
import uuid
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
FIELDS = ["member_id", "region", "membership", "age_band", "visit_bucket"]
PUBLIC_FILTER_FIELDS = ["region", "membership", "age_band", "visit_bucket"]
DATASET_ID = "retail-demo"
DEFAULT_EPSILON_BUDGET = 3.0
# Rounding precision for budget arithmetic, absorbs float representation dust.
_BUDGET_PRECISION = 9


class ReleaseConflictError(Exception):
    """The request_id was already spent on a different normalized request."""


class BudgetExceededError(Exception):
    """The remaining privacy budget cannot cover the requested epsilon."""


def initialize_database(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with closing(sqlite3.connect(path)) as connection, connection:
        connection.execute(
            "CREATE TABLE IF NOT EXISTS datasets ("
            "id TEXT PRIMARY KEY, name TEXT NOT NULL, description TEXT NOT NULL, "
            "synthetic INTEGER NOT NULL, fields_json TEXT NOT NULL)"
        )
        connection.execute(
            "CREATE TABLE IF NOT EXISTS retail_members ("
            "member_id TEXT PRIMARY KEY, region TEXT NOT NULL, "
            "membership TEXT NOT NULL, age_band TEXT NOT NULL, "
            "visit_bucket TEXT NOT NULL)"
        )
        connection.execute(
            "CREATE TABLE IF NOT EXISTS releases ("
            "release_id TEXT PRIMARY KEY, request_id TEXT NOT NULL UNIQUE, "
            "dataset_id TEXT NOT NULL, filters_json TEXT NOT NULL, "
            "epsilon REAL NOT NULL, published_count INTEGER NOT NULL, "
            "remaining_budget REAL NOT NULL, created_at TEXT NOT NULL)"
        )
        if connection.execute(
            "SELECT 1 FROM datasets WHERE id = ?", (DATASET_ID,)
        ).fetchone():
            return
        with (ROOT / "data" / "retail_members.csv").open(
            encoding="utf-8", newline=""
        ) as source:
            rows = list(csv.DictReader(source))
        connection.executemany(
            "INSERT INTO retail_members VALUES (?, ?, ?, ?, ?)",
            [tuple(row[field] for field in FIELDS) for row in rows],
        )
        connection.execute(
            "INSERT INTO datasets VALUES (?, ?, ?, ?, ?)",
            (
                DATASET_ID,
                "合成零售会员数据",
                "固定构造的区域、会员等级、年龄段和到店频率样例。",
                1,
                json.dumps(FIELDS),
            ),
        )


def list_datasets(path: Path) -> list[dict]:
    with closing(sqlite3.connect(path)) as connection:
        connection.row_factory = sqlite3.Row
        records = connection.execute(
            "SELECT id, name, description, synthetic, fields_json "
            "FROM datasets ORDER BY id"
        ).fetchall()
    return [
        {
            "id": row["id"],
            "name": row["name"],
            "description": row["description"],
            "synthetic": bool(row["synthetic"]),
            "fields": json.loads(row["fields_json"]),
        }
        for row in records
    ]


def canonical_filters(filters: dict | None) -> str:
    """Serialize filters so semantically identical requests compare equal."""
    return json.dumps(
        filters or {}, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    )


def _release_from_row(row: sqlite3.Row) -> dict:
    return {
        "release_id": row["release_id"],
        "request_id": row["request_id"],
        "dataset_id": row["dataset_id"],
        "filters": json.loads(row["filters_json"]),
        "epsilon": row["epsilon"],
        "published_count": row["published_count"],
        "remaining_budget": row["remaining_budget"],
        "created_at": row["created_at"],
    }


def count_matching_members(path: Path, filters: dict) -> int:
    clauses = " AND ".join(f"{field} = ?" for field in filters)
    query = "SELECT COUNT(*) FROM retail_members"
    if clauses:
        query += f" WHERE {clauses}"
    with closing(sqlite3.connect(path)) as connection:
        return connection.execute(query, tuple(filters.values())).fetchone()[0]


def create_release(
    path: Path,
    *,
    request_id: str,
    dataset_id: str,
    filters: dict,
    epsilon: float,
    published_count: int,
    budget: float,
) -> tuple[dict, bool]:
    """Record a release, atomically deducting epsilon from the budget.

    Returns (record, created). A replayed request_id with an identical
    normalized request returns the original record with created=False and
    deducts nothing; a different request raises ReleaseConflictError.
    Insufficient budget raises BudgetExceededError and leaves no record.
    """
    filters_json = canonical_filters(filters)
    with closing(sqlite3.connect(path)) as connection:
        connection.row_factory = sqlite3.Row
        connection.isolation_level = None
        connection.execute("BEGIN IMMEDIATE")
        try:
            existing = connection.execute(
                "SELECT * FROM releases WHERE request_id = ?", (request_id,)
            ).fetchone()
            if existing is not None:
                if (
                    existing["dataset_id"] == dataset_id
                    and existing["filters_json"] == filters_json
                    and existing["epsilon"] == epsilon
                ):
                    connection.execute("COMMIT")
                    return _release_from_row(existing), False
                raise ReleaseConflictError(request_id)
            used = connection.execute(
                "SELECT COALESCE(SUM(epsilon), 0.0) FROM releases"
            ).fetchone()[0]
            remaining = round(budget - used - epsilon, _BUDGET_PRECISION)
            if remaining < 0:
                raise BudgetExceededError(request_id)
            record = {
                "release_id": uuid.uuid4().hex,
                "request_id": request_id,
                "dataset_id": dataset_id,
                "filters": json.loads(filters_json),
                "epsilon": epsilon,
                "published_count": published_count,
                "remaining_budget": remaining,
                "created_at": datetime.now(timezone.utc).isoformat(
                    timespec="milliseconds"
                ),
            }
            connection.execute(
                "INSERT INTO releases (release_id, request_id, dataset_id, "
                "filters_json, epsilon, published_count, remaining_budget, "
                "created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    record["release_id"],
                    record["request_id"],
                    record["dataset_id"],
                    filters_json,
                    record["epsilon"],
                    record["published_count"],
                    record["remaining_budget"],
                    record["created_at"],
                ),
            )
            connection.execute("COMMIT")
            return record, True
        except BaseException:
            if connection.in_transaction:
                connection.execute("ROLLBACK")
            raise


def list_releases(path: Path) -> list[dict]:
    with closing(sqlite3.connect(path)) as connection:
        connection.row_factory = sqlite3.Row
        rows = connection.execute(
            "SELECT * FROM releases ORDER BY created_at DESC, rowid DESC"
        ).fetchall()
    return [_release_from_row(row) for row in rows]


def query_releases(
    path: Path,
    *,
    dataset_id: str | None = None,
    request_id: str | None = None,
    start: datetime | None = None,
    end: datetime | None = None,
    limit: int = 50,
) -> list[dict]:
    """Read-only filtered view of successfully published releases.

    Only the releases table is read; member-level data is never touched.
    The window is [start, end) compared against created_at in UTC, and
    results are newest first, capped at ``limit`` rows.
    """
    clauses: list[str] = []
    parameters: list[object] = []
    if dataset_id is not None:
        clauses.append("dataset_id = ?")
        parameters.append(dataset_id)
    if request_id is not None:
        clauses.append("request_id = ?")
        parameters.append(request_id)
    if start is not None:
        clauses.append("created_at >= ?")
        parameters.append(start.astimezone(timezone.utc).isoformat(timespec="milliseconds"))
    if end is not None:
        clauses.append("created_at < ?")
        parameters.append(end.astimezone(timezone.utc).isoformat(timespec="milliseconds"))
    where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
    query = (
        "SELECT * FROM releases"
        + where
        + " ORDER BY created_at DESC, rowid DESC LIMIT ?"
    )
    with closing(sqlite3.connect(path)) as connection:
        connection.row_factory = sqlite3.Row
        rows = connection.execute(query, (*parameters, limit)).fetchall()
    return [_release_from_row(row) for row in rows]


def budget_status(path: Path, budget: float) -> dict:
    with closing(sqlite3.connect(path)) as connection:
        used = connection.execute(
            "SELECT COALESCE(SUM(epsilon), 0.0) FROM releases"
        ).fetchone()[0]
    used = round(used, _BUDGET_PRECISION)
    return {
        "initial_budget": budget,
        "used_budget": used,
        "remaining_budget": max(0.0, round(budget - used, _BUDGET_PRECISION)),
    }
