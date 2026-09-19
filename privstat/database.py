"""SQLite storage for the bundled synthetic data catalog."""

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
# Rounding precision for budget arithmetic, absorbs float representation dust.
_BUDGET_PRECISION = 9


class ReleaseConflictError(Exception):
    """The request_id was already spent on a different normalized request."""


class BudgetExceededError(Exception):
    """The remaining privacy budget cannot cover the requested epsilon."""


class ShareRotationConflictError(Exception):
    """The share is revoked or expired, so its credentials cannot rotate."""


def _utc_iso(moment: datetime) -> str:
    return moment.astimezone(timezone.utc).isoformat(timespec="microseconds")


def hash_token(token: str) -> str:
    """SHA-256 hex digest of a raw share token."""
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


_SHARES_COLUMNS_SQL = (
    "CREATE TABLE IF NOT EXISTS shares ("
    "share_id TEXT PRIMARY KEY, token_digest TEXT NOT NULL UNIQUE, "
    "dataset_id TEXT NOT NULL, request_id TEXT, "
    "from_time TEXT, to_time TEXT, result_limit INTEGER NOT NULL, "
    "created_at TEXT NOT NULL, expires_at TEXT NOT NULL, "
    "revoked_at TEXT)"
)

_SHARES_INSERT_COLUMNS = (
    "share_id, token_digest, dataset_id, request_id, from_time, to_time, "
    "result_limit, created_at, expires_at, revoked_at"
)

# Every successful rotation appends one row here: the new generation's
# digest, its 1-based version (the original token is version 1 and is not
# recorded), and the rotation instant. (share_id, rotation_id) is the
# idempotency key; token_digest is globally unique so a superseded token
# can still be told apart from an unknown one.
_SHARE_ROTATIONS_COLUMNS_SQL = (
    "CREATE TABLE IF NOT EXISTS share_rotations ("
    "share_id TEXT NOT NULL, rotation_id TEXT NOT NULL, "
    "token_digest TEXT NOT NULL UNIQUE, token_version INTEGER NOT NULL, "
    "rotated_at TEXT NOT NULL, "
    "PRIMARY KEY (share_id, rotation_id), "
    "UNIQUE (share_id, token_version))"
)

# Digests of every generation that was once valid but has been rotated
# away — including generation 1, which share_rotations never records.
# This is what lets a rotated-away token read as gone (410) instead of
# unknown (404) after a restart.
_SHARE_SUPERSEDED_COLUMNS_SQL = (
    "CREATE TABLE IF NOT EXISTS share_superseded_tokens ("
    "token_digest TEXT PRIMARY KEY, share_id TEXT NOT NULL)"
)


def _table_columns(connection: sqlite3.Connection, table: str) -> set[str]:
    return {row[1] for row in connection.execute(f"PRAGMA table_info({table})")}


def _migrate_legacy_shares(connection: sqlite3.Connection) -> None:
    """Move plaintext tokens to SHA-256 digests in one atomic transaction.

    Runs inside the caller's transaction: any failure rolls back to the
    original shares table, so a failed migration never loses a share. It is
    idempotent: databases already storing digests are left untouched.
    """
    rows = connection.execute(
        "SELECT share_id, token, dataset_id, request_id, from_time, to_time, "
        "result_limit, created_at, expires_at, revoked_at FROM shares"
    ).fetchall()
    connection.execute("ALTER TABLE shares RENAME TO shares_legacy_plaintext")
    connection.execute(_SHARES_COLUMNS_SQL)
    connection.executemany(
        f"INSERT INTO shares ({_SHARES_INSERT_COLUMNS}) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (
                share_id,
                hash_token(token),
                dataset_id,
                request_id,
                from_time,
                to_time,
                result_limit,
                created_at,
                expires_at,
                revoked_at,
            )
            for (
                share_id,
                token,
                dataset_id,
                request_id,
                from_time,
                to_time,
                result_limit,
                created_at,
                expires_at,
                revoked_at,
            ) in rows
        ],
    )
    connection.execute("DROP TABLE shares_legacy_plaintext")


def _backfill_release_timestamps(connection: sqlite3.Connection) -> None:
    """Canonicalize legacy created_at values to indexable UTC microseconds.

    Existing values (millisecond precision) denote the same instant; the
    rewrite makes lexicographic SQL comparison correct at full resolution.
    """
    legacy = connection.execute("SELECT rowid, created_at FROM releases").fetchall()
    for row_id, created_at in legacy:
        normalized = _utc_iso(datetime.fromisoformat(created_at))
        if normalized != created_at:
            connection.execute(
                "UPDATE releases SET created_at = ? WHERE rowid = ?",
                (normalized, row_id),
            )


def initialize_database(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    migrated_plaintext = False
    # Explicit transaction: Python's sqlite3 does not open one for DDL, so
    # without BEGIN the legacy-table rename in _migrate_legacy_shares would
    # auto-commit and a later failure would lose shares. Inside BEGIN, DDL
    # is transactional and a failure rolls everything back atomically.
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
                "remaining_budget REAL NOT NULL, created_at TEXT NOT NULL)"
            )
            _backfill_release_timestamps(connection)
            connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_releases_created_at "
                "ON releases (created_at DESC)"
            )
            share_columns = _table_columns(connection, "shares")
            if not share_columns:
                connection.execute(_SHARES_COLUMNS_SQL)
            elif "token_digest" not in share_columns:
                _migrate_legacy_shares(connection)
                migrated_plaintext = True
            if "token_version" not in _table_columns(connection, "shares"):
                # Existing rows keep their original token, which is
                # generation 1 by definition.
                connection.execute(
                    "ALTER TABLE shares "
                    "ADD COLUMN token_version INTEGER NOT NULL DEFAULT 1"
                )
            connection.execute(_SHARE_ROTATIONS_COLUMNS_SQL)
            connection.execute(_SHARE_SUPERSEDED_COLUMNS_SQL)
            connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_shares_created_at "
                "ON shares (created_at DESC)"
            )
            if not connection.execute(
                "SELECT 1 FROM datasets WHERE id = ?", (DATASET_ID,)
            ).fetchone():
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
    if migrated_plaintext:
        # Rewrite the file so dropped plaintext tokens do not linger in
        # free pages; VACUUM cannot run inside a transaction.
        with closing(sqlite3.connect(path)) as cleanup:
            cleanup.execute("VACUUM")


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
                "created_at": _utc_iso(datetime.now(timezone.utc)),
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
    results are newest first, capped at ``limit`` rows. All created_at
    values are stored (and backfilled on startup) as canonical UTC
    microsecond ISO strings, so the bounds compare inside the database at
    the actual instants, never truncated to milliseconds.
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
        parameters.append(_utc_iso(start))
    if end is not None:
        clauses.append("created_at < ?")
        parameters.append(_utc_iso(end))
    where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
    query = (
        "SELECT * FROM releases"
        + where
        + " ORDER BY created_at DESC, rowid DESC LIMIT ?"
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


def _share_from_row(row: sqlite3.Row) -> dict:
    return {
        "share_id": row["share_id"],
        "token_digest": row["token_digest"],
        "dataset_id": row["dataset_id"],
        "request_id": row["request_id"],
        "from": row["from_time"],
        "to": row["to_time"],
        "limit": row["result_limit"],
        "created_at": row["created_at"],
        "expires_at": row["expires_at"],
        "revoked_at": row["revoked_at"],
    }


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

    The raw token is an unguessable random string returned to the caller
    exactly once; only its SHA-256 digest is stored. No token or digest
    ever appears in release history or exports.
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
            f"INSERT INTO shares ({_SHARES_INSERT_COLUMNS}) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                record["share_id"],
                hash_token(token),
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
            "SELECT * FROM shares WHERE token_digest = ?", (hash_token(token),)
        ).fetchone()
    return _share_from_row(row) if row is not None else None


def revoke_share(path: Path, token: str) -> bool:
    """Atomically mark a share revoked, looked up by raw token digest.

    Revoking an already-revoked share succeeds again (idempotent); the
    original revocation time is kept. Returns True when a matching share
    exists (including when it was already revoked).
    """
    now = _utc_iso(datetime.now(timezone.utc))
    with closing(sqlite3.connect(path)) as connection:
        connection.isolation_level = None
        connection.execute("BEGIN IMMEDIATE")
        try:
            connection.execute(
                "UPDATE shares SET revoked_at = ? "
                "WHERE token_digest = ? AND revoked_at IS NULL",
                (now, hash_token(token)),
            )
            exists = connection.execute(
                "SELECT 1 FROM shares WHERE token_digest = ?", (hash_token(token),)
            ).fetchone()
            connection.execute("COMMIT")
            return exists is not None
        except BaseException:
            if connection.in_transaction:
                connection.execute("ROLLBACK")
            raise


def revoke_share_by_id(path: Path, share_id: str) -> bool:
    """Atomically revoke a share by share_id; False if no share matches.

    Idempotent: an already-revoked share keeps its original revoked_at and
    still reports success. Concurrent revocations converge to one
    persisted revoked_at.
    """
    now = _utc_iso(datetime.now(timezone.utc))
    with closing(sqlite3.connect(path)) as connection:
        connection.isolation_level = None
        connection.execute("BEGIN IMMEDIATE")
        try:
            connection.execute(
                "UPDATE shares SET revoked_at = ? "
                "WHERE share_id = ? AND revoked_at IS NULL",
                (now, share_id),
            )
            exists = connection.execute(
                "SELECT 1 FROM shares WHERE share_id = ?", (share_id,)
            ).fetchone()
            connection.execute("COMMIT")
            return exists is not None
        except BaseException:
            if connection.in_transaction:
                connection.execute("ROLLBACK")
            raise


def _rotation_from_row(row: sqlite3.Row) -> dict:
    # Never includes the token: replayed rotations cannot reveal it again.
    return {
        "share_id": row["share_id"],
        "rotation_id": row["rotation_id"],
        "token_version": row["token_version"],
        "rotated_at": row["rotated_at"],
    }


def rotate_share(path: Path, *, share_id: str, rotation_id: str) -> tuple[dict, bool] | None:
    """Atomically replace a share's token with a new generation.

    Returns (record, created), or None when no share has this share_id.
    A replayed rotation_id returns the original metadata with
    created=False and never changes state — this check runs before the
    revoked/expired checks so a successful rotation stays repeatable.
    A revoked or expired (per current UTC) share raises
    ShareRotationConflictError and is left untouched. On success the new
    token's SHA-256 digest replaces the current one and the generation
    is appended to share_rotations, all in one transaction, so
    concurrent rotations with the same rotation_id produce exactly one
    new generation and at most one generation is ever valid. The raw
    token appears only in the returned record of the creating call.
    Only the shares and share_rotations tables are touched: no member
    data is read, no budget is deducted, no release record is created.
    """
    now = _utc_iso(datetime.now(timezone.utc))
    with closing(sqlite3.connect(path)) as connection:
        connection.row_factory = sqlite3.Row
        connection.isolation_level = None
        connection.execute("BEGIN IMMEDIATE")
        try:
            share = connection.execute(
                "SELECT token_digest, token_version, revoked_at, expires_at "
                "FROM shares WHERE share_id = ?",
                (share_id,),
            ).fetchone()
            if share is None:
                connection.execute("COMMIT")
                return None
            existing = connection.execute(
                "SELECT * FROM share_rotations "
                "WHERE share_id = ? AND rotation_id = ?",
                (share_id, rotation_id),
            ).fetchone()
            if existing is not None:
                connection.execute("COMMIT")
                return _rotation_from_row(existing), False
            if share["revoked_at"] is not None or share["expires_at"] <= now:
                raise ShareRotationConflictError(share_id)
            token = secrets.token_urlsafe(32)
            digest = hash_token(token)
            record = {
                "share_id": share_id,
                "rotation_id": rotation_id,
                "token": token,
                "token_version": share["token_version"] + 1,
                "rotated_at": now,
            }
            connection.execute(
                "INSERT INTO share_rotations (share_id, rotation_id, "
                "token_digest, token_version, rotated_at) "
                "VALUES (?, ?, ?, ?, ?)",
                (
                    share_id,
                    rotation_id,
                    digest,
                    record["token_version"],
                    now,
                ),
            )
            connection.execute(
                "INSERT INTO share_superseded_tokens (token_digest, share_id) "
                "VALUES (?, ?)",
                (share["token_digest"], share_id),
            )
            connection.execute(
                "UPDATE shares SET token_digest = ?, token_version = ? "
                "WHERE share_id = ?",
                (digest, record["token_version"], share_id),
            )
            connection.execute("COMMIT")
            return record, True
        except BaseException:
            if connection.in_transaction:
                connection.execute("ROLLBACK")
            raise


def is_superseded_token(path: Path, token: str) -> bool:
    """True when the token was a valid earlier generation of some share.

    Every rotation moves the previous generation's digest into
    share_superseded_tokens, so a rotated-away token — including the
    original generation-1 token — reads as gone (410) rather than
    unknown (404).
    """
    with closing(sqlite3.connect(path)) as connection:
        row = connection.execute(
            "SELECT 1 FROM share_superseded_tokens WHERE token_digest = ?",
            (hash_token(token),),
        ).fetchone()
        return row is not None


def _share_status(expires_at: str, revoked_at: str | None, now_iso: str) -> str:
    # Revocation takes precedence over expiry.
    if revoked_at is not None:
        return "revoked"
    if expires_at <= now_iso:
        return "expired"
    return "active"


def list_shares(
    path: Path,
    *,
    status: str | None = None,
    dataset_id: str | None = None,
    request_id: str | None = None,
    limit: int = 50,
) -> list[dict]:
    """List shares newest first, never returning tokens or token digests.

    ``status`` is computed against current UTC entirely in SQL on the
    canonical UTC timestamps; revocation takes precedence over expiry.
    """
    clauses: list[str] = []
    parameters: list[object] = []
    if dataset_id is not None:
        clauses.append("dataset_id = ?")
        parameters.append(dataset_id)
    if request_id is not None:
        clauses.append("request_id = ?")
        parameters.append(request_id)
    if status is not None:
        now_iso = _utc_iso(datetime.now(timezone.utc))
        if status == "revoked":
            clauses.append("revoked_at IS NOT NULL")
        elif status == "expired":
            clauses.append("revoked_at IS NULL AND expires_at <= ?")
            parameters.append(now_iso)
        else:  # active
            clauses.append("revoked_at IS NULL AND expires_at > ?")
            parameters.append(now_iso)
    with closing(sqlite3.connect(path)) as connection:
        connection.row_factory = sqlite3.Row
        where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        rows = connection.execute(
            "SELECT * FROM shares"
            + where
            + " ORDER BY created_at DESC, rowid DESC LIMIT ?",
            (*parameters, limit),
        ).fetchall()
    now = datetime.now(timezone.utc)
    now_iso = _utc_iso(now)
    records: list[dict] = []
    for row in rows:
        share = _share_from_row(row)
        records.append(
            {
                "share_id": share["share_id"],
                "dataset_id": share["dataset_id"],
                "request_id": share["request_id"],
                "from": share["from"],
                "to": share["to"],
                "limit": share["limit"],
                "created_at": share["created_at"],
                "expires_at": share["expires_at"],
                "revoked_at": share["revoked_at"],
                "status": _share_status(
                    share["expires_at"], share["revoked_at"], now_iso
                ),
            }
        )
    return records
