"""SQLite storage for the bundled synthetic data catalog."""

import calendar
import csv
import hashlib
import json
import secrets
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
SHARE_STATUSES = ("active", "expired", "revoked")
# Rounding precision for budget arithmetic, absorbs float representation dust.
_BUDGET_PRECISION = 9
MICROS_PER_SECOND = 1_000_000


class ReleaseConflictError(Exception):
    """The request_id was already spent on a different normalized request."""


class BudgetExceededError(Exception):
    """The remaining privacy budget cannot cover the requested epsilon."""


def _hash_token(token: str) -> str:
    """SHA-256 hex digest of a share token. The token itself is never stored."""
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _epoch_microseconds(value: str | datetime) -> int:
    """Indexable UTC microsecond position of an ISO string or datetime.

    Integer UTC arithmetic keeps bounds exact: float timestamp multiplication
    could round a microsecond-true instant onto the wrong side of a bound.
    """
    if isinstance(value, str):
        value = datetime.fromisoformat(value)
    value = value.astimezone(timezone.utc)
    return calendar.timegm(value.utctimetuple()) * MICROS_PER_SECOND + value.microsecond


def initialize_database(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    purge_plaintext = False
    # An explicit transaction makes the DDL migration atomic: Python's
    # sqlite3 otherwise auto-commits around CREATE/ALTER/DROP, which could
    # leave a half-migrated schema (and threaten existing shares) on failure.
    with closing(sqlite3.connect(path)) as connection:
        connection.isolation_level = None
        connection.execute("BEGIN IMMEDIATE")
        try:
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
                "remaining_budget REAL NOT NULL, created_at TEXT NOT NULL, "
                "created_at_us INTEGER)"
            )
            connection.execute(
                "CREATE TABLE IF NOT EXISTS shares ("
                "share_id TEXT PRIMARY KEY, token TEXT, token_digest TEXT, "
                "dataset_id TEXT NOT NULL, request_id TEXT, "
                "from_time TEXT, to_time TEXT, result_limit INTEGER NOT NULL, "
                "created_at TEXT NOT NULL, expires_at TEXT NOT NULL, "
                "revoked_at TEXT)"
            )
            purge_plaintext = _migrate_schema(connection)
            already_seeded = connection.execute(
                "SELECT 1 FROM datasets WHERE id = ?", (DATASET_ID,)
            ).fetchone()
            if not already_seeded:
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
            connection.execute("COMMIT")
        except BaseException:
            if connection.in_transaction:
                connection.execute("ROLLBACK")
            raise
    if purge_plaintext:
        # The rebuilt table drops plaintext tokens logically; VACUUM rewrites
        # the file so stale database pages cannot retain them either. VACUUM
        # cannot run inside a transaction.
        with closing(sqlite3.connect(path)) as connection:
            connection.execute("VACUUM")


def _table_columns(connection: sqlite3.Connection, table: str) -> set[str]:
    return {row[1] for row in connection.execute(f"PRAGMA table_info({table})")}


def _migrate_schema(connection: sqlite3.Connection) -> bool:
    """Upgrade old databases idempotently without losing any row.

    Old release rows get an indexable UTC microsecond timestamp backfilled;
    old shares stored plaintext tokens and are rebuilt so only the SHA-256
    digest remains. Returns True when plaintext tokens may have sat in file
    pages that need a VACUUM. Failures roll the whole migration back, so
    shares are never lost and the migration is retried on the next startup.
    """
    purge_plaintext = False
    release_columns = _table_columns(connection, "releases")
    if "created_at_us" not in release_columns:
        connection.execute("ALTER TABLE releases ADD COLUMN created_at_us INTEGER")
    legacy_releases = connection.execute(
        "SELECT rowid, created_at FROM releases WHERE created_at_us IS NULL"
    ).fetchall()
    for rowid, created_at in legacy_releases:
        connection.execute(
            "UPDATE releases SET created_at_us = ? WHERE rowid = ?",
            (_epoch_microseconds(created_at), rowid),
        )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_releases_created_at_us "
        "ON releases(created_at_us)"
    )

    share_columns = _table_columns(connection, "shares")
    if "token_digest" not in share_columns:
        # The legacy column was ``token TEXT NOT NULL UNIQUE``; a table rebuild
        # is the only way to drop the raw token and its constraints portably.
        legacy_rows = connection.execute(
            "SELECT share_id, token, dataset_id, request_id, from_time, to_time, "
            "result_limit, created_at, expires_at, revoked_at FROM shares"
        ).fetchall()
        connection.execute(
            "CREATE TABLE shares_new ("
            "share_id TEXT PRIMARY KEY, token TEXT, token_digest TEXT, "
            "dataset_id TEXT NOT NULL, request_id TEXT, "
            "from_time TEXT, to_time TEXT, result_limit INTEGER NOT NULL, "
            "created_at TEXT NOT NULL, expires_at TEXT NOT NULL, revoked_at TEXT)"
        )
        connection.executemany(
            "INSERT INTO shares_new (share_id, token, token_digest, dataset_id, "
            "request_id, from_time, to_time, result_limit, created_at, "
            "expires_at, revoked_at) VALUES (?, NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [(row[0], _hash_token(row[1]), *row[2:]) for row in legacy_rows],
        )
        connection.execute("DROP TABLE shares")
        connection.execute("ALTER TABLE shares_new RENAME TO shares")
        purge_plaintext = bool(legacy_rows)
    elif connection.execute(
        "SELECT 1 FROM shares WHERE token IS NOT NULL LIMIT 1"
    ).fetchone():
        # Defensive: digest any plaintext token left behind (nullable column).
        for share_id, token in connection.execute(
            "SELECT share_id, token FROM shares WHERE token IS NOT NULL"
        ).fetchall():
            connection.execute(
                "UPDATE shares SET token_digest = ?, token = NULL "
                "WHERE share_id = ?",
                (_hash_token(token), share_id),
            )
        purge_plaintext = True
    connection.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_shares_token_digest "
        "ON shares(token_digest) WHERE token_digest IS NOT NULL"
    )
    return purge_plaintext


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
            created_at = datetime.now(timezone.utc).isoformat(
                timespec="microseconds"
            )
            record = {
                "release_id": uuid.uuid4().hex,
                "request_id": request_id,
                "dataset_id": dataset_id,
                "filters": json.loads(filters_json),
                "epsilon": epsilon,
                "published_count": published_count,
                "remaining_budget": remaining,
                "created_at": created_at,
            }
            connection.execute(
                "INSERT INTO releases (release_id, request_id, dataset_id, "
                "filters_json, epsilon, published_count, remaining_budget, "
                "created_at, created_at_us) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    record["release_id"],
                    record["request_id"],
                    record["dataset_id"],
                    filters_json,
                    record["epsilon"],
                    record["published_count"],
                    record["remaining_budget"],
                    record["created_at"],
                    _epoch_microseconds(created_at),
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
            "SELECT * FROM releases ORDER BY created_at_us DESC, rowid DESC"
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
    The window is [start, end) compared against created_at in UTC using the
    indexable microsecond column, results are newest first and capped at
    ``limit`` rows. Time bounds are compared at full precision against the
    actual stored instants, never truncated to milliseconds.
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
        clauses.append("created_at_us >= ?")
        parameters.append(_epoch_microseconds(start.astimezone(timezone.utc)))
    if end is not None:
        clauses.append("created_at_us < ?")
        parameters.append(_epoch_microseconds(end.astimezone(timezone.utc)))
    where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
    query = (
        "SELECT * FROM releases"
        + where
        + " ORDER BY created_at_us DESC, rowid DESC LIMIT ?"
    )
    parameters.append(limit)
    with closing(sqlite3.connect(path)) as connection:
        connection.row_factory = sqlite3.Row
        rows = connection.execute(query, tuple(parameters)).fetchall()
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


def _utc_iso(moment: datetime) -> str:
    return moment.astimezone(timezone.utc).isoformat(timespec="microseconds")


def _share_view(row: sqlite3.Row, status: str) -> dict:
    """Management-safe share fields: never includes the token or its digest."""
    return {
        "share_id": row["share_id"],
        "dataset_id": row["dataset_id"],
        "request_id": row["request_id"],
        "from": row["from_time"],
        "to": row["to_time"],
        "limit": row["result_limit"],
        "created_at": row["created_at"],
        "expires_at": row["expires_at"],
        "revoked_at": row["revoked_at"],
        "status": status,
    }


def _current_share_status(row: sqlite3.Row, now: datetime) -> str:
    # Revocation takes precedence over expiry.
    if row["revoked_at"] is not None:
        return "revoked"
    if datetime.fromisoformat(row["expires_at"]) <= now:
        return "expired"
    return "active"


def create_share(
    path: Path,
    *,
    dataset_id: str,
    request_id: str | None,
    start: datetime | None,
    end: datetime | None,
    limit: int,
    expires_at: datetime,
) -> dict:
    """Persist a share scope and return the stored record.

    The raw token is returned once to the caller and never written to disk;
    only its SHA-256 digest is stored. Tokens never enter release history or
    exports.
    """
    token = secrets.token_urlsafe(32)
    record = {
        "share_id": uuid.uuid4().hex,
        "token": token,
        "dataset_id": dataset_id,
        "request_id": request_id,
        "from": _utc_iso(start) if start is not None else None,
        "to": _utc_iso(end) if end is not None else None,
        "limit": limit,
        "created_at": _utc_iso(datetime.now(timezone.utc)),
        "expires_at": _utc_iso(expires_at),
        "revoked_at": None,
    }
    with closing(sqlite3.connect(path)) as connection, connection:
        connection.execute(
            "INSERT INTO shares (share_id, token, token_digest, dataset_id, "
            "request_id, from_time, to_time, result_limit, created_at, "
            "expires_at, revoked_at) VALUES (?, NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                record["share_id"],
                _hash_token(token),
                record["dataset_id"],
                record["request_id"],
                record["from"],
                record["to"],
                record["limit"],
                record["created_at"],
                record["expires_at"],
                record["revoked_at"],
            ),
        )
    return record


def get_share_by_token(path: Path, token: str) -> dict | None:
    with closing(sqlite3.connect(path)) as connection:
        connection.row_factory = sqlite3.Row
        row = connection.execute(
            "SELECT * FROM shares WHERE token_digest = ?", (_hash_token(token),)
        ).fetchone()
    if row is None:
        return None
    return _share_view(row, _current_share_status(row, datetime.now(timezone.utc)))


def list_shares(
    path: Path,
    *,
    status: str | None = None,
    dataset_id: str | None = None,
    request_id: str | None = None,
    limit: int = 50,
) -> list[dict]:
    """List shares newest first, excluding the token and its digest.

    ``status`` is computed against the current UTC instant; revocation takes
    precedence over expiry.
    """
    clauses: list[str] = []
    parameters: list[object] = []
    if dataset_id is not None:
        clauses.append("dataset_id = ?")
        parameters.append(dataset_id)
    if request_id is not None:
        clauses.append("request_id = ?")
        parameters.append(request_id)
    if status == "revoked":
        clauses.append("revoked_at IS NOT NULL")
    elif status in ("active", "expired"):
        clauses.append("revoked_at IS NULL")
    where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
    query = (
        "SELECT * FROM shares" + where + " ORDER BY created_at DESC, rowid DESC"
    )
    now = datetime.now(timezone.utc)
    records: list[dict] = []
    with closing(sqlite3.connect(path)) as connection:
        connection.row_factory = sqlite3.Row
        rows = connection.execute(query, tuple(parameters)).fetchall()
    for row in rows:
        current = _current_share_status(row, now)
        if status is not None and current != status:
            continue
        records.append(_share_view(row, current))
        if len(records) >= limit:
            break
    return records


def revoke_share_by_id(path: Path, share_id: str) -> bool:
    """Atomically mark a share revoked. Returns False for an unknown share_id.

    Revoking an already-revoked share succeeds again (idempotent); the
    original revocation time is kept and concurrent callers persist exactly
    one revocation state.
    """
    now = _utc_iso(datetime.now(timezone.utc))
    with closing(sqlite3.connect(path)) as connection:
        connection.isolation_level = None
        connection.execute("BEGIN IMMEDIATE")
        try:
            exists = connection.execute(
                "SELECT 1 FROM shares WHERE share_id = ?", (share_id,)
            ).fetchone()
            connection.execute(
                "UPDATE shares SET revoked_at = ? "
                "WHERE share_id = ? AND revoked_at IS NULL",
                (now, share_id),
            )
            connection.execute("COMMIT")
        except BaseException:
            if connection.in_transaction:
                connection.execute("ROLLBACK")
            raise
    return exists is not None


def revoke_share_by_token(path: Path, token: str) -> None:
    """Idempotently revoke by raw token.

    Unknown tokens also succeed: revoking is a state assertion, so callers
    always get a committed end state. Concurrent calls persist one revoked_at.
    """
    now = _utc_iso(datetime.now(timezone.utc))
    with closing(sqlite3.connect(path)) as connection:
        connection.isolation_level = None
        connection.execute("BEGIN IMMEDIATE")
        try:
            connection.execute(
                "UPDATE shares SET revoked_at = ? "
                "WHERE token_digest = ? AND revoked_at IS NULL",
                (now, _hash_token(token)),
            )
            connection.execute("COMMIT")
        except BaseException:
            if connection.in_transaction:
                connection.execute("ROLLBACK")
            raise
