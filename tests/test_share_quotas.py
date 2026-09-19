import json
import sqlite3
import tempfile
import unittest
import uuid
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock
from urllib.request import urlopen

from privstat.database import (
    ShareQuotaExhaustedError,
    claim_served_access,
    create_share as db_create_share,
    get_share_by_token,
    hash_token,
    initialize_database,
    list_share_access_events,
)
from privstat.demo import running_demo

from test_shares import create_share, future_iso, publish, request


def create_share_http(base_url, **overrides):
    status, body = create_share(base_url, **overrides)
    assert status == 201, body
    return json.loads(body)


def list_events(base_url, query=""):
    suffix = f"?{query}" if query else ""
    status, body = request(base_url, "GET", f"/api/share-access-events{suffix}")
    return status, json.loads(body)


class QuotaCreateTests(unittest.TestCase):
    def test_omitted_or_null_means_unlimited(self):
        with running_demo(0) as base_url:
            for payload_extra in ({}, {"max_accesses": None}):
                status, body = create_share(base_url, **payload_extra)
                self.assertEqual(status, 201, body)
                share = json.loads(body)
                self.assertIsNone(share["max_accesses"])
                self.assertEqual(share["served_count"], 0)
                self.assertIsNone(share["remaining_accesses"])

    def test_bounds_1_and_1000_accepted(self):
        with running_demo(0) as base_url:
            for value in (1, 1000):
                status, body = create_share(base_url, max_accesses=value)
                self.assertEqual(status, 201, body)
                share = json.loads(body)
                self.assertEqual(share["max_accesses"], value)
                self.assertEqual(share["remaining_accesses"], value)

    def test_invalid_max_accesses_returns_422(self):
        with running_demo(0) as base_url:
            for value in (0, -1, 1001, 1.5, True, False, "3", [], {}):
                with self.subTest(value=value):
                    status, _ = create_share(base_url, max_accesses=value)
                    self.assertEqual(status, 422)

    def test_listing_reports_quota_fields(self):
        with running_demo(0) as base_url:
            share = create_share_http(base_url, max_accesses=4)
            status, body = request(base_url, "GET", "/api/shares?limit=100")
            self.assertEqual(status, 200)
            listed = [
                s for s in json.loads(body) if s["share_id"] == share["share_id"]
            ][0]
            self.assertEqual(listed["max_accesses"], 4)
            self.assertEqual(listed["served_count"], 0)
            self.assertEqual(listed["remaining_accesses"], 4)
            # Tokens still never appear in the listing.
            self.assertNotIn(share["token"], body)
            self.assertNotIn(hash_token(share["token"]), body)


class QuotaAccessTests(unittest.TestCase):
    def test_exactly_max_successes_then_429_without_data(self):
        with running_demo(0) as base_url:
            publish(base_url, "quota-rows", filters={"region": "north"})
            share = create_share_http(base_url, request_id="quota-rows", max_accesses=3)
            token = share["token"]
            for index in range(3):
                status, body = request(
                    base_url, "GET", f"/api/shares/{token}/releases"
                )
                self.assertEqual(status, 200, body)
                self.assertEqual(len(json.loads(body)), 1)
            # Every later visit is rejected with 429 and carries no rows.
            for _ in range(2):
                status, body = request(
                    base_url, "GET", f"/api/shares/{token}/releases"
                )
                self.assertEqual(status, 429)
                self.assertNotIn("quota-rows", body)
                self.assertNotIn(share["token"], body)
            status, events = list_events(
                base_url, f"share_id={share['share_id']}&limit=100"
            )
            self.assertEqual([e["outcome"] for e in events].count("served"), 3)
            self.assertEqual(
                [e["outcome"] for e in events].count("quota_exhausted"), 2
            )
            # Every served audit carries the real response count; every
            # exhaustion audit carries zero.
            served = [e for e in events if e["outcome"] == "served"]
            exhausted = [e for e in events if e["outcome"] == "quota_exhausted"]
            self.assertTrue(all(e["result_count"] == 1 for e in served))
            self.assertTrue(all(e["result_count"] == 0 for e in exhausted))
            status, body = request(base_url, "GET", "/api/shares?limit=100")
            listed = [
                s for s in json.loads(body) if s["share_id"] == share["share_id"]
            ][0]
            self.assertEqual(listed["served_count"], 3)
            self.assertEqual(listed["remaining_accesses"], 0)

    def test_unlimited_share_always_succeeds(self):
        with running_demo(0) as base_url:
            publish(base_url, "quota-unlimited")
            share = create_share_http(base_url, request_id="quota-unlimited")
            for _ in range(12):
                status, _ = request(
                    base_url, "GET", f"/api/shares/{share['token']}/releases"
                )
                self.assertEqual(status, 200)
            status, body = request(base_url, "GET", "/api/shares?limit=100")
            listed = [
                s for s in json.loads(body) if s["share_id"] == share["share_id"]
            ][0]
            self.assertIsNone(listed["max_accesses"])
            self.assertIsNone(listed["remaining_accesses"])
            self.assertEqual(listed["served_count"], 12)
            status, events = list_events(
                base_url, f"share_id={share['share_id']}&limit=100"
            )
            self.assertTrue(all(e["outcome"] == "served" for e in events))

    def test_concurrent_accesses_have_exactly_max_successes(self):
        with running_demo(0) as base_url:
            publish(base_url, "quota-concurrent")
            share = create_share_http(
                base_url, request_id="quota-concurrent", max_accesses=10
            )
            url = f"{base_url}/api/shares/{share['token']}/releases"

            def access(_):
                try:
                    with urlopen(url, timeout=10) as response:
                        response.read()
                        return response.status
                except Exception as error:  # HTTPError carries the 429
                    code = getattr(error, "code", None)
                    close = getattr(error, "close", None)
                    if close is not None:
                        close()
                    return code

            with ThreadPoolExecutor(max_workers=16) as pool:
                statuses = list(pool.map(access, range(30)))
            self.assertEqual(statuses.count(200), 10)
            self.assertEqual(statuses.count(429), 20)
            status, body = request(base_url, "GET", "/api/shares?limit=100")
            listed = [
                s for s in json.loads(body) if s["share_id"] == share["share_id"]
            ][0]
            self.assertEqual(listed["served_count"], 10)
            self.assertEqual(listed["remaining_accesses"], 0)
            status, events = list_events(
                base_url, f"share_id={share['share_id']}&limit=100"
            )
            self.assertEqual(sum(e["outcome"] == "served" for e in events), 10)
            self.assertEqual(
                sum(e["outcome"] == "quota_exhausted" for e in events), 20
            )
            self.assertEqual(len({e["event_id"] for e in events}), 30)

    def test_rejection_precedence_runs_before_quota(self):
        with running_demo(0) as base_url:
            publish(base_url, "quota-precedence")
            # A spent, then revoked share reports revoked (410), not 429.
            share = create_share_http(
                base_url, request_id="quota-precedence", max_accesses=1
            )
            status, _ = request(
                base_url, "GET", f"/api/shares/{share['token']}/releases"
            )
            self.assertEqual(status, 200)
            status, _ = request(
                base_url, "GET", f"/api/shares/{share['token']}/releases"
            )
            self.assertEqual(status, 429)
            status, _ = request(
                base_url, "DELETE", f"/api/shares/id/{share['share_id']}"
            )
            self.assertEqual(status, 204)
            status, _ = request(
                base_url, "GET", f"/api/shares/{share['token']}/releases"
            )
            self.assertEqual(status, 410)
            status, events = list_events(
                base_url, f"share_id={share['share_id']}&limit=100"
            )
            outcomes = [e["outcome"] for e in events]
            self.assertEqual(outcomes.count("revoked"), 1)
            self.assertIn("quota_exhausted", outcomes)
            # Rejections never spend quota: only the one success counted.
            status, body = request(base_url, "GET", "/api/shares?limit=100")
            listed = [
                s for s in json.loads(body) if s["share_id"] == share["share_id"]
            ][0]
            self.assertEqual(listed["served_count"], 1)

    def test_old_generation_is_superseded_before_quota_but_new_token_keeps_quota(self):
        with running_demo(0) as base_url:
            publish(base_url, "quota-rotate")
            share = create_share_http(
                base_url, request_id="quota-rotate", max_accesses=1
            )
            old_token = share["token"]
            status, _ = request(
                base_url, "GET", f"/api/shares/{old_token}/releases"
            )
            self.assertEqual(status, 200)
            # Rotation does not reset the counter: the old generation is
            # superseded (410, no quota spent) and the new one is exhausted.
            status, body = request(
                base_url,
                "POST",
                f"/api/shares/id/{share['share_id']}/rotate",
                {"rotation_id": "quota-rot-1"},
            )
            self.assertEqual(status, 201)
            new_token = json.loads(body)["token"]
            status, _ = request(
                base_url, "GET", f"/api/shares/{old_token}/releases"
            )
            self.assertEqual(status, 410)
            status, _ = request(
                base_url, "GET", f"/api/shares/{new_token}/releases"
            )
            self.assertEqual(status, 429)
            status, events = list_events(
                base_url, f"share_id={share['share_id']}&limit=100"
            )
            outcomes = {e["outcome"] for e in events}
            self.assertIn("superseded", outcomes)
            self.assertIn("quota_exhausted", outcomes)
            self.assertEqual(sum(e["outcome"] == "served" for e in events), 1)

    def test_expired_share_with_spent_quota_reports_expired(self):
        with running_demo(0) as base_url:
            publish(base_url, "quota-expired")
            share = create_share_http(
                base_url,
                request_id="quota-expired",
                max_accesses=1,
                expires_at=future_iso(seconds=1),
            )
            status, _ = request(
                base_url, "GET", f"/api/shares/{share['token']}/releases"
            )
            self.assertEqual(status, 200)
            import time

            time.sleep(1.2)
            status, _ = request(
                base_url, "GET", f"/api/shares/{share['token']}/releases"
            )
            self.assertEqual(status, 410)
            status, events = list_events(
                base_url, f"share_id={share['share_id']}&limit=100"
            )
            self.assertEqual([e["outcome"] for e in events][0], "expired")

    def test_quota_exhausted_filter_and_unknown_token_unchanged(self):
        with running_demo(0) as base_url:
            share = create_share_http(base_url, max_accesses=1)
            status, _ = request(
                base_url, "GET", f"/api/shares/{share['token']}/releases"
            )
            self.assertEqual(status, 200)
            status, _ = request(
                base_url, "GET", f"/api/shares/{share['token']}/releases"
            )
            self.assertEqual(status, 429)
            status, events = list_events(base_url, "outcome=quota_exhausted")
            self.assertEqual(status, 200)
            self.assertTrue(events)
            self.assertTrue(all(e["outcome"] == "quota_exhausted" for e in events))
            for bad in ("outcome=quota", "outcome=QUOTA_EXHAUSTED"):
                status, _ = list_events(base_url, bad)
                self.assertEqual(status, 422)
            # Unknown tokens remain 404 and never enter the audit trail.
            status, _ = request(
                base_url, "GET", "/api/shares/no-such-token/releases"
            )
            self.assertEqual(status, 404)
            status, events = list_events(
                base_url, "outcome=quota_exhausted&share_id=does-not-exist"
            )
            self.assertEqual(events, [])

    def test_failed_claim_write_rolls_back_counter_and_audit(self):
        with running_demo(0) as base_url:
            publish(base_url, "quota-rollback")
            share = create_share_http(
                base_url, request_id="quota-rollback", max_accesses=2
            )
            status, _ = request(
                base_url, "GET", f"/api/shares/{share['token']}/releases"
            )
            self.assertEqual(status, 200)
            # Fail inside the claim transaction: the counter increment and
            # the audit row must roll back together.
            with mock.patch(
                "privstat.database.uuid.uuid4",
                side_effect=RuntimeError("audit storage unavailable"),
            ):
                status, body = request(
                    base_url, "GET", f"/api/shares/{share['token']}/releases"
                )
                self.assertEqual(status, 500)
                self.assertNotIn("quota-rollback", body)
            # No quota was spent by the failed attempt: one success remains.
            status, _ = request(
                base_url, "GET", f"/api/shares/{share['token']}/releases"
            )
            self.assertEqual(status, 200)
            status, _ = request(
                base_url, "GET", f"/api/shares/{share['token']}/releases"
            )
            self.assertEqual(status, 429)
            status, events = list_events(
                base_url, f"share_id={share['share_id']}&limit=100"
            )
            self.assertEqual(sum(e["outcome"] == "served" for e in events), 2)


class QuotaStorageTests(unittest.TestCase):
    def _new_share(self, path, **kwargs):
        params = {
            "dataset_id": "retail-demo",
            "request_id": None,
            "start": None,
            "end": None,
            "limit": 10,
            "expires_at": datetime.now(timezone.utc) + timedelta(hours=1),
        }
        params.update(kwargs)
        return db_create_share(path, **params)

    def test_claim_increments_and_then_exhausts(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.sqlite3"
            initialize_database(path)
            share = self._new_share(path, max_accesses=2)
            first = claim_served_access(
                path,
                share_id=share["share_id"],
                token_version=1,
                result_count=3,
            )
            self.assertEqual(first["event"]["outcome"], "served")
            self.assertEqual(first["event"]["result_count"], 3)
            self.assertEqual(first["served_count"], 1)
            second = claim_served_access(
                path,
                share_id=share["share_id"],
                token_version=1,
                result_count=0,
            )
            self.assertEqual(second["served_count"], 2)
            with self.assertRaises(ShareQuotaExhaustedError) as caught:
                claim_served_access(
                    path,
                    share_id=share["share_id"],
                    token_version=1,
                    result_count=5,
                )
            event = caught.exception.event
            self.assertEqual(event["outcome"], "quota_exhausted")
            self.assertEqual(event["result_count"], 0)
            # The exhausted visit does not advance the counter.
            with closing(sqlite3.connect(path)) as connection:
                served_count = connection.execute(
                    "SELECT served_count FROM shares WHERE share_id = ?",
                    (share["share_id"],),
                ).fetchone()[0]
                rows = connection.execute(
                    "SELECT outcome, result_count FROM share_access_events "
                    "ORDER BY rowid"
                ).fetchall()
            self.assertEqual(served_count, 2)
            self.assertEqual(
                rows, [("served", 3), ("served", 0), ("quota_exhausted", 0)]
            )

    def test_unlimited_claim_never_exhausts(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.sqlite3"
            initialize_database(path)
            share = self._new_share(path)
            for _ in range(20):
                result = claim_served_access(
                    path,
                    share_id=share["share_id"],
                    token_version=1,
                    result_count=0,
                )
                self.assertEqual(result["event"]["outcome"], "served")
            with closing(sqlite3.connect(path)) as connection:
                max_accesses, served_count = connection.execute(
                    "SELECT max_accesses, served_count FROM shares"
                ).fetchone()
            self.assertIsNone(max_accesses)
            self.assertEqual(served_count, 20)

    def test_concurrent_claims_commit_exactly_max_successes(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.sqlite3"
            initialize_database(path)
            share = self._new_share(path, max_accesses=5)

            def claim(_):
                try:
                    claim_served_access(
                        path,
                        share_id=share["share_id"],
                        token_version=1,
                        result_count=1,
                    )
                    return "served"
                except ShareQuotaExhaustedError:
                    return "quota_exhausted"

            with ThreadPoolExecutor(max_workers=8) as pool:
                outcomes = list(pool.map(claim, range(20)))
            self.assertEqual(outcomes.count("served"), 5)
            self.assertEqual(outcomes.count("quota_exhausted"), 15)
            with closing(sqlite3.connect(path)) as connection:
                served_count = connection.execute(
                    "SELECT served_count FROM shares WHERE share_id = ?",
                    (share["share_id"],),
                ).fetchone()[0]
                counts = dict(
                    connection.execute(
                        "SELECT outcome, COUNT(*) FROM share_access_events GROUP BY outcome"
                    ).fetchall()
                )
            self.assertEqual(served_count, 5)
            self.assertEqual(counts, {"served": 5, "quota_exhausted": 15})

    def test_failed_insert_rolls_back_the_counter(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.sqlite3"
            initialize_database(path)
            share = self._new_share(path, max_accesses=3)
            claim_served_access(
                path,
                share_id=share["share_id"],
                token_version=1,
                result_count=1,
            )
            duplicate_hex = uuid.uuid4().hex
            # Consume the id first so the next INSERT collides after the
            # UPDATE already ran inside the claim transaction.
            with closing(sqlite3.connect(path)) as connection, connection:
                connection.execute(
                    "INSERT INTO share_access_events "
                    "(event_id, share_id, token_version, outcome, result_count, "
                    "accessed_at) VALUES (?, ?, ?, ?, ?, ?)",
                    (duplicate_hex, share["share_id"], 1, "served", 1,
                     "2026-01-01T00:00:00.000000+00:00"),
                )
            with mock.patch(
                "privstat.database.uuid.uuid4",
                return_value=mock.Mock(hex=duplicate_hex),
            ):
                with self.assertRaises(sqlite3.IntegrityError):
                    claim_served_access(
                        path,
                        share_id=share["share_id"],
                        token_version=1,
                        result_count=1,
                    )
            # Rolled back: still one success spent, and two more fit.
            claim_served_access(
                path,
                share_id=share["share_id"],
                token_version=1,
                result_count=1,
            )
            with closing(sqlite3.connect(path)) as connection:
                served_count = connection.execute(
                    "SELECT served_count FROM shares WHERE share_id = ?",
                    (share["share_id"],),
                ).fetchone()[0]
            self.assertEqual(served_count, 2)

    def test_legacy_digest_database_migrates_to_unlimited_zero_used(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.sqlite3"
            initialize_database(path)
            share = self._new_share(path, request_id="legacy-quota")
            # Rebuild the shares table as it shipped before access quotas:
            # the ten digest-era columns, no quota columns.
            with closing(sqlite3.connect(path)) as connection, connection:
                connection.execute("ALTER TABLE shares RENAME TO shares_old")
                connection.execute(
                    "CREATE TABLE shares ("
                    "share_id TEXT PRIMARY KEY, token_digest TEXT NOT NULL UNIQUE, "
                    "dataset_id TEXT NOT NULL, request_id TEXT, "
                    "from_time TEXT, to_time TEXT, result_limit INTEGER NOT NULL, "
                    "created_at TEXT NOT NULL, expires_at TEXT NOT NULL, revoked_at TEXT)"
                )
                connection.execute(
                    "INSERT INTO shares "
                    "SELECT share_id, token_digest, dataset_id, request_id, "
                    "from_time, to_time, result_limit, created_at, expires_at, "
                    "revoked_at FROM shares_old"
                )
                connection.execute("DROP TABLE shares_old")
            # Restart migration: unlimited, zero uses; scope/generation kept.
            initialize_database(path)
            initialize_database(path)  # idempotent second run
            with closing(sqlite3.connect(path)) as connection:
                columns = {
                    row[1]
                    for row in connection.execute("PRAGMA table_info(shares)")
                }
                row = connection.execute(
                    "SELECT max_accesses, served_count, request_id, result_limit "
                    "FROM shares"
                ).fetchone()
            self.assertIn("max_accesses", columns)
            self.assertIn("served_count", columns)
            self.assertEqual(row[0], None)
            self.assertEqual(row[1], 0)
            self.assertEqual(row[2], "legacy-quota")
            self.assertEqual(row[3], 10)
            resolved = get_share_by_token(path, share["token"])
            self.assertIsNotNone(resolved)
            self.assertTrue(resolved["token_current"])
            self.assertIsNone(resolved["max_accesses"])
            self.assertEqual(resolved["served_count"], 0)
            # A migrated legacy share behaves as unlimited.
            claim_served_access(
                path,
                share_id=share["share_id"],
                token_version=1,
                result_count=0,
            )

    def test_legacy_plaintext_database_migrates_to_unlimited(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.sqlite3"
            initialize_database(path)
            raw_token = "legacy-plaintext-quota-token"
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
                        "legacy-plaintext-share",
                        raw_token,
                        "retail-demo",
                        "legacy-plaintext-req",
                        None,
                        None,
                        5,
                        "2026-01-01T00:00:00+00:00",
                        "2030-01-01T00:00:00+00:00",
                        None,
                    ),
                )
            initialize_database(path)
            share = get_share_by_token(path, raw_token)
            self.assertIsNotNone(share)
            self.assertEqual(share["share_id"], "legacy-plaintext-share")
            self.assertIsNone(share["max_accesses"])
            self.assertEqual(share["served_count"], 0)
            events = list_share_access_events(path, limit=50)
            self.assertEqual(events, [])


if __name__ == "__main__":
    unittest.main()
