import calendar
import contextlib
import hashlib
import json
import sqlite3
import tempfile
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import uvicorn

from privstat.app import create_app
from privstat.database import (
    FIELDS,
    initialize_database,
    revoke_share_by_id,
    revoke_share_by_token,
)
from privstat.demo import running_demo


LIST_FIELDS = {
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


def future_iso(**kwargs):
    return (datetime.now(timezone.utc) + timedelta(**kwargs)).isoformat()


def request(base_url, method, path, payload=None):
    data = json.dumps(payload).encode("utf-8") if payload is not None else None
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


def create_share(base_url, **overrides):
    payload = {"dataset_id": "retail-demo", "expires_at": future_iso(hours=1)}
    payload.update(overrides)
    return request(base_url, "POST", "/api/shares", payload)


def list_shares(base_url, query=""):
    suffix = f"?{query}" if query else ""
    return request(base_url, "GET", f"/api/shares{suffix}")


@contextlib.contextmanager
def running_app_at(path: Path):
    import socket

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(("127.0.0.1", 0))
        listener.listen(128)
        server = uvicorn.Server(
            uvicorn.Config(create_app(path), log_level="warning")
        )
        worker = threading.Thread(
            target=server.run, kwargs={"sockets": [listener]}
        )
        worker.start()
        try:
            deadline = time.monotonic() + 15
            while not server.started:
                if not worker.is_alive() or time.monotonic() >= deadline:
                    raise RuntimeError("PrivStat service did not start.")
                time.sleep(0.05)
            yield f"http://127.0.0.1:{listener.getsockname()[1]}"
        finally:
            server.should_exit = True
            worker.join(timeout=10)


def build_legacy_database(path: Path, token: str) -> None:
    """Create a database in the pre-migration shape (plaintext tokens)."""
    import json as _json

    created_at = "2026-03-01T12:00:00.123000+00:00"
    expires_at = "2030-01-01T00:00:00.000000+00:00"
    with closing(sqlite3.connect(path)) as connection, connection:
        connection.execute(
            "CREATE TABLE datasets ("
            "id TEXT PRIMARY KEY, name TEXT NOT NULL, description TEXT NOT NULL, "
            "synthetic INTEGER NOT NULL, fields_json TEXT NOT NULL)"
        )
        connection.execute(
            "CREATE TABLE retail_members ("
            "member_id TEXT PRIMARY KEY, region TEXT NOT NULL, "
            "membership TEXT NOT NULL, age_band TEXT NOT NULL, "
            "visit_bucket TEXT NOT NULL)"
        )
        connection.execute(
            "CREATE TABLE releases ("
            "release_id TEXT PRIMARY KEY, request_id TEXT NOT NULL UNIQUE, "
            "dataset_id TEXT NOT NULL, filters_json TEXT NOT NULL, "
            "epsilon REAL NOT NULL, published_count INTEGER NOT NULL, "
            "remaining_budget REAL NOT NULL, created_at TEXT NOT NULL)"
        )
        connection.execute(
            "CREATE TABLE shares ("
            "share_id TEXT PRIMARY KEY, token TEXT NOT NULL UNIQUE, "
            "dataset_id TEXT NOT NULL, request_id TEXT, "
            "from_time TEXT, to_time TEXT, result_limit INTEGER NOT NULL, "
            "created_at TEXT NOT NULL, expires_at TEXT NOT NULL, revoked_at TEXT)"
        )
        connection.execute(
            "INSERT INTO datasets VALUES (?, ?, ?, ?, ?)",
            ("retail-demo", "legacy", "old schema", 1, _json.dumps(FIELDS)),
        )
        connection.execute(
            "INSERT INTO releases VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                "rel-legacy-1",
                "legacy-req",
                "retail-demo",
                "{}",
                0.2,
                7,
                2.8,
                created_at,
            ),
        )
        connection.execute(
            "INSERT INTO shares VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                "share-legacy-1",
                token,
                "retail-demo",
                "legacy-req",
                None,
                None,
                50,
                created_at,
                expires_at,
                None,
            ),
        )


class ShareListTests(unittest.TestCase):
    def test_lists_management_fields_newest_first_without_token(self):
        with running_demo(0) as base_url:
            status, first_body = create_share(base_url, request_id="list-a")
            self.assertEqual(status, 201)
            first = json.loads(first_body)
            time.sleep(0.01)
            status, second_body = create_share(base_url, request_id="list-b")
            self.assertEqual(status, 201)
            second = json.loads(second_body)
            status, body = list_shares(base_url)
            self.assertEqual(status, 200)
            shares = json.loads(body)
            self.assertEqual([s["share_id"] for s in shares[:2]], [second["share_id"], first["share_id"]])
            for share in shares:
                self.assertEqual(set(share), LIST_FIELDS)
                self.assertNotIn("token", share)
            # Neither raw token nor its digest may surface in the response.
            for created in (first, second):
                self.assertNotIn(created["token"], body)
                digest = hashlib.sha256(created["token"].encode()).hexdigest()
                self.assertNotIn(digest, body)

    def test_filters_are_exact(self):
        with running_demo(0) as base_url:
            status, one = create_share(base_url, request_id="filter-me")
            self.assertEqual(status, 201)
            create_share(base_url, request_id="other")
            status, body = list_shares(base_url, "request_id=filter-me")
            self.assertEqual(status, 200)
            shares = json.loads(body)
            self.assertEqual([s["share_id"] for s in shares], [json.loads(one)["share_id"]])
            status, body = list_shares(base_url, "request_id=missing")
            self.assertEqual(status, 200)
            self.assertEqual(json.loads(body), [])
            status, body = list_shares(base_url, "request_id=")
            self.assertEqual(status, 200)
            self.assertEqual(json.loads(body), [])
            status, _ = list_shares(base_url, "request_id=%20%20")
            self.assertEqual(status, 422)
            status, body = list_shares(base_url, "dataset_id=retail-demo")
            self.assertEqual(status, 200)
            self.assertGreaterEqual(len(json.loads(body)), 2)
            for invalid in ("dataset_id=other-dataset", "dataset_id=", "dataset_id=%20%20"):
                status, _ = list_shares(base_url, invalid)
                self.assertEqual(status, 422, invalid)

    def test_invalid_status_and_limit_return_422(self):
        with running_demo(0) as base_url:
            create_share(base_url)
            for query in ("status=deleted", "limit=0", "limit=101", "limit=abc"):
                with self.subTest(query=query):
                    status, _ = list_shares(base_url, query)
                    self.assertEqual(status, 422)

    def test_limit_is_applied_newest_first(self):
        with running_demo(0) as base_url:
            ids = []
            for index in range(3):
                status, body = create_share(base_url, request_id=f"cap-{index}")
                ids.append(json.loads(body)["share_id"])
                time.sleep(0.01)
            status, body = list_shares(base_url, "limit=2")
            self.assertEqual(status, 200)
            self.assertEqual([s["share_id"] for s in json.loads(body)], ids[::-1][:2])

    def test_status_reflects_now_with_revocation_precedence(self):
        with running_demo(0) as base_url:
            # Expired share.
            status, expired = create_share(base_url, expires_at=future_iso(seconds=1))
            self.assertEqual(status, 201)
            expired_id = json.loads(expired)["share_id"]
            # Revoked and expired: must classify as revoked.
            status, both = create_share(
                base_url, request_id="both", expires_at=future_iso(seconds=1)
            )
            self.assertEqual(status, 201)
            both_share = json.loads(both)
            # Plain active share.
            status, active = create_share(base_url, request_id="active-one")
            self.assertEqual(status, 201)
            active_id = json.loads(active)["share_id"]
            status, _ = request(
                base_url, "DELETE", f"/api/shares/id/{both_share['share_id']}"
            )
            self.assertEqual(status, 204)
            time.sleep(1.2)

            status, body = list_shares(base_url, "status=active")
            active_ids = {s["share_id"] for s in json.loads(body)}
            self.assertIn(active_id, active_ids)
            self.assertNotIn(expired_id, active_ids)
            self.assertNotIn(both_share["share_id"], active_ids)

            status, body = list_shares(base_url, "status=expired")
            expired_rows = json.loads(body)
            self.assertEqual([s["share_id"] for s in expired_rows], [expired_id])
            self.assertEqual(expired_rows[0]["revoked_at"], None)

            status, body = list_shares(base_url, "status=revoked")
            revoked = {s["share_id"]: s for s in json.loads(body)}
            self.assertIn(both_share["share_id"], revoked)
            self.assertEqual(revoked[both_share["share_id"]]["status"], "revoked")
            self.assertIsNotNone(revoked[both_share["share_id"]]["revoked_at"])


class ShareIdRevokeTests(unittest.TestCase):
    def test_revoke_by_id_is_idempotent_unknown_404_and_blocks_access(self):
        with running_demo(0) as base_url:
            status, body = create_share(base_url)
            self.assertEqual(status, 201)
            share = json.loads(body)
            status, _ = request(
                base_url, "DELETE", f"/api/shares/id/{share['share_id']}"
            )
            self.assertEqual(status, 204)
            status, body = request(
                base_url, "DELETE", f"/api/shares/id/{share['share_id']}"
            )
            self.assertEqual(status, 204)
            self.assertEqual(body, "")
            status, _ = request(base_url, "DELETE", "/api/shares/id/no-such-id")
            self.assertEqual(status, 404)
            status, _ = request(
                base_url, "GET", f"/api/shares/{share['token']}/releases"
            )
            self.assertEqual(status, 410)

    def test_concurrent_revocations_persist_one_state(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.sqlite3"
            initialize_database(path)
            from privstat.database import create_share

            share = create_share(
                path,
                dataset_id="retail-demo",
                request_id=None,
                start=None,
                end=None,
                limit=10,
                expires_at=datetime.now(timezone.utc) + timedelta(hours=1),
            )

            def revoke_by_id(_):
                return revoke_share_by_id(path, share["share_id"])

            def revoke_by_token(_):
                revoke_share_by_token(path, share["token"])

            with ThreadPoolExecutor(max_workers=8) as pool:
                id_results = list(pool.map(revoke_by_id, range(6)))
                list(pool.map(revoke_by_token, range(4)))
            self.assertTrue(all(id_results))
            with closing(sqlite3.connect(path)) as connection:
                row = connection.execute(
                    "SELECT token, token_digest, revoked_at FROM shares "
                    "WHERE share_id = ?",
                    (share["share_id"],),
                ).fetchone()
            self.assertIsNone(row[0])
            self.assertEqual(
                row[1], hashlib.sha256(share["token"].encode()).hexdigest()
            )
            self.assertIsNotNone(row[2])
            revoked_at = row[2]
            # Repeated revokes keep the single committed timestamp.
            revoke_share_by_id(path, share["share_id"])
            revoke_share_by_token(path, share["token"])
            with closing(sqlite3.connect(path)) as connection:
                again = connection.execute(
                    "SELECT revoked_at FROM shares WHERE share_id = ?",
                    (share["share_id"],),
                ).fetchone()[0]
            self.assertEqual(again, revoked_at)


class PlaintextTokenMigrationTests(unittest.TestCase):
    def test_legacy_plaintext_share_still_works_and_digest_is_purged(self):
        token = "legacy-plaintext-token-0123456789abcdef-xyz"
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "legacy.sqlite3"
            build_legacy_database(path, token)
            # A failed migration attempt must not destroy the share.
            from privstat import database

            original = database._hash_token
            def failing_hash(value):
                raise RuntimeError("simulated migration failure")
            database._hash_token = failing_hash
            try:
                with self.assertRaises(RuntimeError):
                    initialize_database(path)
            finally:
                database._hash_token = original
            with closing(sqlite3.connect(path)) as connection:
                survived = connection.execute(
                    "SELECT share_id, token FROM shares"
                ).fetchall()
            self.assertEqual(survived, [("share-legacy-1", token)])

            # Retrying the migration succeeds and is idempotent.
            initialize_database(path)
            initialize_database(path)
            with closing(sqlite3.connect(path)) as connection:
                row = connection.execute(
                    "SELECT token, token_digest, revoked_at FROM shares"
                ).fetchone()
                release_us = connection.execute(
                    "SELECT created_at, created_at_us FROM releases"
                ).fetchone()
            self.assertIsNone(row[0])
            self.assertEqual(
                row[1], hashlib.sha256(token.encode()).hexdigest()
            )
            expected_us = (
                calendar.timegm(
                    datetime.fromisoformat(release_us[0])
                    .astimezone(timezone.utc)
                    .utctimetuple()
                )
                * 1_000_000
                + datetime.fromisoformat(release_us[0])
                .astimezone(timezone.utc)
                .microsecond
            )
            self.assertEqual(release_us[1], expected_us)
            # VACUUM rewrote the file: no plaintext token lingers in pages.
            self.assertNotIn(token.encode(), path.read_bytes())

            with running_app_at(path) as base_url:
                status, body = request(
                    base_url, "GET", f"/api/shares/{token}/releases"
                )
                self.assertEqual(status, 200)
                records = json.loads(body)
                self.assertEqual([r["request_id"] for r in records], ["legacy-req"])
                self.assertNotIn(token, body)

                status, body = list_shares(base_url)
                self.assertEqual(status, 200)
                self.assertNotIn(token, body)
                self.assertNotIn(row[1], body)

                # Old links can still be revoked by token after migration.
                status, _ = request(base_url, "DELETE", f"/api/shares/{token}")
                self.assertEqual(status, 204)
                status, _ = request(
                    base_url, "GET", f"/api/shares/{token}/releases"
                )
                self.assertEqual(status, 410)

                # Export keeps [from, to) precision against the backfilled
                # microsecond column for legacy releases.
                from urllib.parse import quote

                cut = quote(release_us[0], safe="")
                status, body = request(
                    base_url, "GET", "/api/releases/export?" + f"to={cut}"
                )
                self.assertEqual(status, 200)
                self.assertEqual(json.loads(body), [])
                status, body = request(
                    base_url, "GET", "/api/releases/export?" + f"from={cut}"
                )
                self.assertEqual(
                    [r["request_id"] for r in json.loads(body)], ["legacy-req"]
                )


class ManagementIsReadOnlyTests(unittest.TestCase):
    def test_listing_and_revoking_do_not_touch_budget_or_releases(self):
        with running_demo(0) as base_url:
            with urlopen(base_url + "/api/privacy-budget", timeout=10) as response:
                budget_before = json.load(response)
            with urlopen(base_url + "/api/releases", timeout=10) as response:
                history_before = json.load(response)
            status, body = create_share(base_url)
            self.assertEqual(status, 201)
            share_id = json.loads(body)["share_id"]
            for query in ("", "status=active", "limit=1"):
                status, _ = list_shares(base_url, query)
                self.assertEqual(status, 200)
            status, _ = request(base_url, "DELETE", f"/api/shares/id/{share_id}")
            self.assertEqual(status, 204)
            with urlopen(base_url + "/api/privacy-budget", timeout=10) as response:
                budget_after = json.load(response)
            with urlopen(base_url + "/api/releases", timeout=10) as response:
                history_after = json.load(response)
            self.assertEqual(budget_before, budget_after)
            self.assertEqual(history_before, history_after)


if __name__ == "__main__":
    unittest.main()
