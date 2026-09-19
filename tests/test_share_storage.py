import hashlib
import json
import sqlite3
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock
from urllib.request import Request

from privstat.database import (
    create_share,
    get_share_by_token,
    hash_token,
    initialize_database,
    list_shares,
    query_releases,
    revoke_share,
    revoke_share_by_id,
)
from privstat.demo import running_demo


MANAGED_FIELDS = {
    "share_id",
    "dataset_id",
    "request_id",
    "from",
    "to",
    "limit",
    "created_at",
    "expires_at",
    "revoked_at",
    "status",
}


def request(base_url, method, path, payload=None):
    import json as _json
    from urllib.error import HTTPError
    from urllib.request import urlopen

    data = _json.dumps(payload).encode("utf-8") if payload is not None else None
    headers = {"Content-Type": "application/json"} if data is not None else {}
    req = Request(base_url + path, data=data, headers=headers, method=method)
    try:
        with urlopen(req, timeout=10) as response:
            return response.status, response.read().decode("utf-8")
    except HTTPError as error:
        body = error.read().decode("utf-8")
        status = error.code
        error.close()
        return status, body


def future_iso(**kwargs):
    return (datetime.now(timezone.utc) + timedelta(**kwargs)).isoformat()


def create_share_http(base_url, **overrides):
    payload = {"dataset_id": "retail-demo", "expires_at": future_iso(hours=1)}
    payload.update(overrides)
    status, body = request(base_url, "POST", "/api/shares", payload)
    assert status == 201, body
    return json.loads(body)


class ShareListEndpointTests(unittest.TestCase):
    def test_list_newest_first_with_status_and_no_token_fields(self):
        with running_demo(0) as base_url:
            first = create_share_http(base_url, request_id="list-1")
            second = create_share_http(base_url, request_id="list-2")
            third = create_share_http(base_url, request_id="list-3")
            status, body = request(base_url, "GET", "/api/shares")
            self.assertEqual(status, 200)
            shares = json.loads(body)
            self.assertEqual([s["share_id"] for s in shares[:3]], [
                third["share_id"], second["share_id"], first["share_id"]
            ])
            for share in shares:
                self.assertEqual(set(share), MANAGED_FIELDS)
                self.assertNotIn("token", share)
                self.assertNotIn("token_digest", share)
                self.assertEqual(share["status"], "active")
            # Neither the raw token nor its digest may appear anywhere.
            self.assertNotIn(first["token"], body)
            self.assertNotIn(hash_token(first["token"]), body)

    def test_limit_caps_results(self):
        with running_demo(0) as base_url:
            create_share_http(base_url, request_id="lim-1")
            create_share_http(base_url, request_id="lim-2")
            create_share_http(base_url, request_id="lim-3")
            status, body = request(base_url, "GET", "/api/shares?limit=2")
            self.assertEqual(status, 200)
            shares = json.loads(body)
            self.assertEqual(len(shares), 2)
            self.assertEqual([s["request_id"] for s in shares], ["lim-3", "lim-2"])

    def test_filters(self):
        with running_demo(0) as base_url:
            create_share_http(base_url, request_id="filt-a")
            target = create_share_http(base_url, request_id="filt-b")
            status, body = request(base_url, "GET", "/api/shares?request_id=filt-b")
            self.assertEqual(status, 200)
            shares = json.loads(body)
            self.assertEqual([s["share_id"] for s in shares], [target["share_id"]])
            status, body = request(base_url, "GET", "/api/shares?dataset_id=retail-demo")
            self.assertEqual(status, 200)
            self.assertGreaterEqual(len(json.loads(body)), 2)
            status, body = request(base_url, "GET", "/api/shares?request_id=missing")
            self.assertEqual(status, 200)
            self.assertEqual(json.loads(body), [])

    def test_invalid_parameters_return_422(self):
        with running_demo(0) as base_url:
            create_share_http(base_url)
            for query in (
                "status=unknown",
                "dataset_id=other-dataset",
                "dataset_id=%20%20",
                "request_id=",
                "request_id=%20%20",
                "limit=0",
                "limit=101",
                "limit=abc",
            ):
                with self.subTest(query=query):
                    status, _ = request(base_url, "GET", f"/api/shares?{query}")
                    self.assertEqual(status, 422)

    def test_status_filters_expired_and_revoked_with_revocation_precedence(self):
        with running_demo(0) as base_url:
            # Expired, not revoked.
            expired = create_share_http(
                base_url, request_id="st-expired", expires_at=future_iso(seconds=1)
            )
            # Will be revoked before it expires; then also left past expiry.
            revoking = create_share_http(
                base_url, request_id="st-revoked", expires_at=future_iso(seconds=1)
            )
            active = create_share_http(base_url, request_id="st-active")
            status, _ = request(
                base_url, "DELETE", f"/api/shares/id/{revoking['share_id']}"
            )
            self.assertEqual(status, 204)

            def ids(status_filter):
                status, body = request(
                    base_url, "GET", f"/api/shares?status={status_filter}&limit=100"
                )
                self.assertEqual(status, 200)
                return {s["request_id"] for s in json.loads(body)}

            self.assertEqual(ids("revoked"), {"st-revoked"})
            # A revoked share is never counted as expired while it ticks down.
            self.assertNotIn("st-revoked", ids("expired"))

            import time
            time.sleep(1.2)
            # Only the long-lived share remains active.
            self.assertEqual(ids("active"), {"st-active"})
            # Expired share now shows under expired...
            self.assertIn("st-expired", ids("expired"))
            # ...and a revoked-but-also-expired share stays revoked.
            self.assertIn("st-revoked", ids("revoked"))
            self.assertNotIn("st-revoked", ids("expired"))
            revoked_rows = [
                s for s in json.loads(
                    request(base_url, "GET", "/api/shares?status=revoked&limit=100")[1]
                ) if s["share_id"] == revoking["share_id"]
            ]
            self.assertEqual(revoked_rows[0]["status"], "revoked")
            self.assertIsNotNone(revoked_rows[0]["revoked_at"])


class RevokeByIdEndpointTests(unittest.TestCase):
    def test_revoke_by_id_is_idempotent_404_unknown_and_blocks_access(self):
        with running_demo(0) as base_url:
            share = create_share_http(base_url)
            status, body = request(
                base_url, "GET", f"/api/shares/{share['token']}/releases"
            )
            self.assertEqual(status, 200)
            status, body = request(
                base_url, "DELETE", f"/api/shares/id/{share['share_id']}"
            )
            self.assertEqual(status, 204)
            self.assertEqual(body, "")
            # Idempotent on the same share_id.
            status, _ = request(
                base_url, "DELETE", f"/api/shares/id/{share['share_id']}"
            )
            self.assertEqual(status, 204)
            # Unknown share_id is 404.
            status, _ = request(base_url, "DELETE", "/api/shares/id/no-such-id")
            self.assertEqual(status, 404)
            # Access after commit is gone.
            status, _ = request(
                base_url, "GET", f"/api/shares/{share['token']}/releases"
            )
            self.assertEqual(status, 410)
            status, listing = request(base_url, "GET", "/api/shares?status=revoked")
            self.assertEqual(status, 200)
            self.assertIn(share["share_id"], [s["share_id"] for s in json.loads(listing)])


class DigestStorageTests(unittest.TestCase):
    def test_only_sha256_digest_is_stored(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.sqlite3"
            initialize_database(path)
            record = create_share(
                path,
                dataset_id="retail-demo",
                request_id="dig-1",
                start=None,
                end=None,
                limit=10,
                expires_at=datetime.now(timezone.utc) + timedelta(hours=1),
            )
            token = record["token"]
            with closing(sqlite3.connect(path)) as connection:
                columns = {row[1] for row in connection.execute("PRAGMA table_info(shares)")}
                self.assertIn("token_digest", columns)
                self.assertNotIn("token", columns)
                stored = connection.execute(
                    "SELECT token_digest FROM shares"
                ).fetchone()[0]
            self.assertEqual(stored, hashlib.sha256(token.encode("utf-8")).hexdigest())
            self.assertNotEqual(stored, token)
            # Lookups and revocation still work with the raw token.
            self.assertIsNotNone(get_share_by_token(path, token))
            self.assertTrue(revoke_share(path, token))
            self.assertIsNone(get_share_by_token(path, "wrong-token"))

    def _build_legacy_database(self, path: Path, token: str) -> None:
        initialize_database(path)
        with closing(sqlite3.connect(path)) as connection, connection:
            connection.execute("DROP TABLE shares")
            connection.execute(
                "CREATE TABLE shares ("
                "share_id TEXT PRIMARY KEY, token TEXT NOT NULL UNIQUE, "
                "dataset_id TEXT NOT NULL, request_id TEXT, "
                "from_time TEXT, to_time TEXT, result_limit INTEGER NOT NULL, "
                "created_at TEXT NOT NULL, expires_at TEXT NOT NULL, revoked_at TEXT)"
            )
            connection.execute(
                "INSERT INTO shares VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    "legacy-share-1",
                    token,
                    "retail-demo",
                    "legacy-req",
                    None,
                    None,
                    5,
                    "2026-01-01T00:00:00+00:00",
                    "2030-01-01T00:00:00+00:00",
                    None,
                ),
            )

    def test_startup_migrates_plaintext_tokens_idempotently(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.sqlite3"
            raw_token = "legacy-plaintext-token-value"
            self._build_legacy_database(path, raw_token)
            initialize_database(path)
            # The old link still resolves and revokes.
            share = get_share_by_token(path, raw_token)
            self.assertIsNotNone(share)
            self.assertEqual(share["share_id"], "legacy-share-1")
            with closing(sqlite3.connect(path)) as connection:
                columns = {row[1] for row in connection.execute("PRAGMA table_info(shares)")}
                self.assertNotIn("token", columns)
                stored = connection.execute(
                    "SELECT token_digest FROM shares"
                ).fetchone()[0]
            self.assertEqual(stored, hash_token(raw_token))
            # Migrating again is a no-op and the share survives.
            initialize_database(path)
            self.assertIsNotNone(get_share_by_token(path, raw_token))
            self.assertTrue(revoke_share(path, raw_token))
            initialize_database(path)
            self.assertIsNotNone(get_share_by_token(path, raw_token))
            # VACUUM after migration removes plaintext remnants from the file.
            self.assertNotIn(raw_token.encode("utf-8"), path.read_bytes())

    def test_failed_migration_never_loses_shares(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.sqlite3"
            raw_token = "legacy-plaintext-token-value"
            self._build_legacy_database(path, raw_token)

            def boom(_token):
                raise RuntimeError("simulated migration failure")

            with mock.patch("privstat.database.hash_token", side_effect=boom):
                with self.assertRaises(RuntimeError):
                    initialize_database(path)
            # The original plaintext table is fully intact after rollback.
            with closing(sqlite3.connect(path)) as connection:
                columns = [
                    row[1]
                    for row in connection.execute("PRAGMA table_info(shares)")
                ]
                row = connection.execute(
                    "SELECT share_id, token FROM shares"
                ).fetchone()
                tables = {
                    row[0]
                    for row in connection.execute(
                        "SELECT name FROM sqlite_master WHERE type='table'"
                    )
                }
            self.assertIn("token", columns)
            self.assertEqual(row, ("legacy-share-1", raw_token))
            self.assertNotIn("shares_legacy_plaintext", tables)
            # A later successful startup migrates and the link keeps working.
            initialize_database(path)
            self.assertEqual(
                get_share_by_token(path, raw_token)["share_id"], "legacy-share-1"
            )

    def test_concurrent_revocation_converges_to_one_state(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.sqlite3"
            initialize_database(path)
            record = create_share(
                path,
                dataset_id="retail-demo",
                request_id="conc-revoke",
                start=None,
                end=None,
                limit=10,
                expires_at=datetime.now(timezone.utc) + timedelta(hours=1),
            )
            with ThreadPoolExecutor(max_workers=8) as pool:
                by_id = list(pool.map(
                    lambda _: revoke_share_by_id(path, record["share_id"]), range(8)
                ))
                by_token = list(pool.map(
                    lambda _: revoke_share(path, record["token"]), range(8)
                ))
            self.assertTrue(all(by_id))
            self.assertTrue(all(by_token))
            with closing(sqlite3.connect(path)) as connection:
                rows = connection.execute(
                    "SELECT DISTINCT revoked_at FROM shares WHERE share_id = ?",
                    (record["share_id"],),
                ).fetchall()
            self.assertEqual(len(rows), 1)
            self.assertIsNotNone(rows[0][0])

    def test_list_shares_never_contains_token_or_digest(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.sqlite3"
            initialize_database(path)
            create_share(
                path,
                dataset_id="retail-demo",
                request_id=None,
                start=None,
                end=None,
                limit=10,
                expires_at=datetime.now(timezone.utc) + timedelta(hours=1),
            )
            for share in list_shares(path):
                self.assertNotIn("token", share)
                self.assertNotIn("token_digest", share)


class ReleaseTimestampBackfillTests(unittest.TestCase):
    def test_legacy_millisecond_timestamps_are_backfilled_and_filtered_in_db(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.sqlite3"
            initialize_database(path)
            legacy_values = [
                ("rel-1", "2026-03-01T10:00:00.123000+00:00"),
                ("rel-2", "2026-03-01T10:00:00.456000+00:00"),
            ]
            with closing(sqlite3.connect(path)) as connection, connection:
                for request_id, created_at in legacy_values:
                    connection.execute(
                        "INSERT INTO releases (release_id, request_id, dataset_id, "
                        "filters_json, epsilon, published_count, remaining_budget, "
                        "created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                        (request_id, request_id, "retail-demo", "{}", 0.1, 1, 2.9,
                         created_at[:23] + "+00:00"),
                    )
            initialize_database(path)
            with closing(sqlite3.connect(path)) as connection:
                stored = {
                    row[0]: row[1]
                    for row in connection.execute(
                        "SELECT request_id, created_at FROM releases"
                    )
                }
                indexes = {
                    row[0]
                    for row in connection.execute(
                        "SELECT name FROM sqlite_master WHERE type='index'"
                    )
                }
            self.assertEqual(stored["rel-1"], "2026-03-01T10:00:00.123000+00:00")
            self.assertIn("idx_releases_created_at", indexes)
            cut = datetime.fromisoformat("2026-03-01T10:00:00.456000+00:00")
            self.assertEqual(
                [r["request_id"] for r in query_releases(
                    path, end=cut, limit=50
                )],
                ["rel-1"],
            )
            self.assertEqual(
                [r["request_id"] for r in query_releases(
                    path, start=cut, limit=50
                )],
                ["rel-2"],
            )
            self.assertEqual(
                [r["request_id"] for r in query_releases(path, limit=50)],
                ["rel-2", "rel-1"],
            )


if __name__ == "__main__":
    unittest.main()
