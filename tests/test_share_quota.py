import json
import sqlite3
import tempfile
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock
from urllib.request import Request, urlopen

from privstat.database import (
    SHARE_ACCESS_OUTCOMES,
    create_share,
    get_share_by_token,
    hash_token,
    initialize_database,
    list_share_access_events,
    list_shares,
    serve_share_access,
)
from privstat.demo import running_demo

from test_shares import (
    create_share as http_create_share,
    future_iso,
    publish,
    request,
)


MANAGED_QUOTA_FIELDS = {"max_accesses", "served_count", "remaining_accesses"}


def create_share_http(base_url, **overrides):
    status, body = http_create_share(base_url, **overrides)
    assert status == 201, body
    return json.loads(body)


def list_events(base_url, query=""):
    suffix = f"?{query}" if query else ""
    status, body = request(
        base_url, "GET", f"/api/share-access-events{suffix}"
    )
    return status, json.loads(body) if body else None


class QuotaCreateTests(unittest.TestCase):
    def test_limited_quota_is_echoed_with_zero_usage(self):
        with running_demo(0) as base_url:
            share = create_share_http(base_url, max_accesses=3)
            self.assertEqual(share["max_accesses"], 3)
            self.assertEqual(share["served_count"], 0)
            self.assertEqual(share["remaining_accesses"], 3)
            status, body = request(base_url, "GET", "/api/shares")
            self.assertEqual(status, 200)
            listed = [
                s for s in json.loads(body) if s["share_id"] == share["share_id"]
            ][0]
            self.assertTrue(MANAGED_QUOTA_FIELDS <= set(listed))
            self.assertEqual(listed["max_accesses"], 3)
            self.assertEqual(listed["served_count"], 0)
            self.assertEqual(listed["remaining_accesses"], 3)

    def test_omitted_and_null_both_mean_unlimited(self):
        with running_demo(0) as base_url:
            omitted = create_share_http(base_url)
            explicit = create_share_http(base_url, max_accesses=None)
            for share in (omitted, explicit):
                self.assertIsNone(share["max_accesses"])
                self.assertEqual(share["served_count"], 0)
                self.assertIsNone(share["remaining_accesses"])

    def test_boundaries_one_and_one_thousand_are_accepted(self):
        with running_demo(0) as base_url:
            for value in (1, 1000):
                status, body = http_create_share(base_url, max_accesses=value)
                self.assertEqual(status, 201, body)
                self.assertEqual(json.loads(body)["max_accesses"], value)


class QuotaAccessTests(unittest.TestCase):
    def test_exactly_max_accesses_succeed_then_429_without_data(self):
        with running_demo(0) as base_url:
            publish(base_url, "quota-basic", filters={"region": "north"})
            share = create_share_http(
                base_url, request_id="quota-basic", max_accesses=2
            )
            for index in range(2):
                status, body = request(
                    base_url, "GET", f"/api/shares/{share['token']}/releases"
                )
                self.assertEqual(status, 200)
                records = json.loads(body)
                self.assertEqual([r["request_id"] for r in records], ["quota-basic"])
            # Third attempt: quota spent, no rows accompany the 429.
            status, body = request(
                base_url, "GET", f"/api/shares/{share['token']}/releases"
            )
            self.assertEqual(status, 429)
            self.assertNotIn("quota-basic", body)
            self.assertNotIn(share["token"], body)
            # Staying exhausted keeps returning 429.
            status, _ = request(
                base_url, "GET", f"/api/shares/{share['token']}/releases"
            )
            self.assertEqual(status, 429)
            status, managed = request(base_url, "GET", "/api/shares")
            row = [
                s for s in json.loads(managed) if s["share_id"] == share["share_id"]
            ][0]
            self.assertEqual(row["served_count"], 2)
            self.assertEqual(row["remaining_accesses"], 0)
            events = list_events(base_url, f"share_id={share['share_id']}&limit=100")[1]
            self.assertEqual(
                [e["outcome"] for e in events],
                ["quota_exhausted", "quota_exhausted", "served", "served"],
            )
            served = [e for e in events if e["outcome"] == "served"]
            exhausted = [e for e in events if e["outcome"] == "quota_exhausted"]
            self.assertTrue(all(e["result_count"] == 1 for e in served))
            self.assertTrue(all(e["result_count"] == 0 for e in exhausted))

    def test_served_result_count_still_equals_response_length(self):
        with running_demo(0) as base_url:
            publish(base_url, "quota-count-1")
            publish(base_url, "quota-count-2")
            # A zero-row successful serve still spends one access.
            empty = create_share_http(
                base_url, request_id="quota-missing", max_accesses=1
            )
            status, body = request(
                base_url, "GET", f"/api/shares/{empty['token']}/releases"
            )
            self.assertEqual(status, 200)
            self.assertEqual(json.loads(body), [])
            status, _ = request(
                base_url, "GET", f"/api/shares/{empty['token']}/releases"
            )
            self.assertEqual(status, 429)
            events = list_events(base_url, f"share_id={empty['share_id']}")[1]
            self.assertEqual(
                [(e["outcome"], e["result_count"]) for e in events],
                [("quota_exhausted", 0), ("served", 0)],
            )

    def test_unlimited_share_never_exhausts(self):
        with running_demo(0) as base_url:
            publish(base_url, "quota-unlimited")
            share = create_share_http(base_url, request_id="quota-unlimited")
            for _ in range(12):
                status, _ = request(
                    base_url, "GET", f"/api/shares/{share['token']}/releases"
                )
                self.assertEqual(status, 200)
            status, managed = request(base_url, "GET", "/api/shares")
            row = [
                s for s in json.loads(managed) if s["share_id"] == share["share_id"]
            ][0]
            self.assertIsNone(row["max_accesses"])
            self.assertEqual(row["served_count"], 12)
            self.assertIsNone(row["remaining_accesses"])
            events = list_events(base_url, f"share_id={share['share_id']}&limit=100")[1]
            self.assertTrue(all(e["outcome"] == "served" for e in events))
            self.assertEqual(len(events), 12)

    def test_revoked_expired_superseded_take_precedence_and_cost_no_quota(self):
        with running_demo(0) as base_url:
            publish(base_url, "quota-precedence")
            # Revoked beats quota: rejection is 410 revoked and counter stays.
            revoked = create_share_http(base_url, max_accesses=1)
            status, _ = request(
                base_url, "DELETE", f"/api/shares/id/{revoked['share_id']}"
            )
            self.assertEqual(status, 204)
            status, _ = request(
                base_url, "GET", f"/api/shares/{revoked['token']}/releases"
            )
            self.assertEqual(status, 410)
            events = list_events(base_url, f"share_id={revoked['share_id']}")[1]
            self.assertEqual([e["outcome"] for e in events], ["revoked"])

            # Expiry beats quota as well.
            expired = create_share_http(
                base_url, max_accesses=1, expires_at=future_iso(seconds=1)
            )
            time.sleep(1.2)
            status, _ = request(
                base_url, "GET", f"/api/shares/{expired['token']}/releases"
            )
            self.assertEqual(status, 410)
            events = list_events(base_url, f"share_id={expired['share_id']}")[1]
            self.assertEqual([e["outcome"] for e in events], ["expired"])

            # An old generation is superseded and does not spend quota; the
            # new generation keeps the full remaining quota.
            rotated = create_share_http(
                base_url, request_id="quota-precedence", max_accesses=1
            )
            old_token = rotated["token"]
            status, body = request(
                base_url,
                "POST",
                f"/api/shares/id/{rotated['share_id']}/rotate",
                {"rotation_id": "quota-rot"},
            )
            self.assertEqual(status, 201)
            new_token = json.loads(body)["token"]
            status, _ = request(
                base_url, "GET", f"/api/shares/{old_token}/releases"
            )
            self.assertEqual(status, 410)
            events = list_events(base_url, f"share_id={rotated['share_id']}&limit=10")[1]
            self.assertIn("superseded", [e["outcome"] for e in events])
            status, _ = request(
                base_url, "GET", f"/api/shares/{new_token}/releases"
            )
            self.assertEqual(status, 200)
            status, _ = request(
                base_url, "GET", f"/api/shares/{new_token}/releases"
            )
            self.assertEqual(status, 429)

    def test_rotation_does_not_reset_consumed_quota(self):
        with running_demo(0) as base_url:
            publish(base_url, "quota-rotate")
            share = create_share_http(
                base_url, request_id="quota-rotate", max_accesses=2
            )
            status, _ = request(
                base_url, "GET", f"/api/shares/{share['token']}/releases"
            )
            self.assertEqual(status, 200)
            status, body = request(
                base_url,
                "POST",
                f"/api/shares/id/{share['share_id']}/rotate",
                {"rotation_id": "quota-rot-reset"},
            )
            self.assertEqual(status, 201)
            new_token = json.loads(body)["token"]
            # One access was already spent before rotation: only one left.
            status, _ = request(
                base_url, "GET", f"/api/shares/{new_token}/releases"
            )
            self.assertEqual(status, 200)
            status, _ = request(
                base_url, "GET", f"/api/shares/{new_token}/releases"
            )
            self.assertEqual(status, 429)
            row = [
                s for s in json.loads(request(base_url, "GET", "/api/shares")[1])
                if s["share_id"] == share["share_id"]
            ][0]
            self.assertEqual(row["served_count"], 2)
            self.assertEqual(row["remaining_accesses"], 0)

    def test_concurrent_accesses_win_exactly_max_times(self):
        with running_demo(0) as base_url:
            publish(base_url, "quota-concurrent")
            share = create_share_http(
                base_url, request_id="quota-concurrent", max_accesses=10
            )

            def access(_):
                try:
                    with urlopen(
                        base_url + f"/api/shares/{share['token']}/releases",
                        timeout=15,
                    ) as response:
                        response.read()
                        return response.status
                except Exception as error:  # HTTPError for 429
                    return error.code

            with ThreadPoolExecutor(max_workers=16) as pool:
                statuses = list(pool.map(access, range(40)))
            self.assertEqual(statuses.count(200), 10)
            self.assertEqual(statuses.count(429), 30)
            events = list_events(
                base_url, f"share_id={share['share_id']}&limit=100"
            )[1]
            self.assertEqual(sum(e["outcome"] == "served" for e in events), 10)
            self.assertEqual(
                sum(e["outcome"] == "quota_exhausted" for e in events), 30
            )
            row = [
                s for s in json.loads(request(base_url, "GET", "/api/shares")[1])
                if s["share_id"] == share["share_id"]
            ][0]
            self.assertEqual(row["served_count"], 10)
            # No budget or history side effects from the audit path.
            with urlopen(base_url + "/api/privacy-budget", timeout=10) as response:
                self.assertEqual(json.load(response)["used_budget"], 0.2)

    def test_failed_quota_write_returns_500_and_spends_nothing(self):
        with running_demo(0) as base_url:
            publish(base_url, "quota-write-fails")
            share = create_share_http(
                base_url, request_id="quota-write-fails", max_accesses=1
            )
            with mock.patch(
                "privstat.app.serve_share_access",
                side_effect=RuntimeError("quota storage unavailable"),
            ):
                status, body = request(
                    base_url, "GET", f"/api/shares/{share['token']}/releases"
                )
                self.assertEqual(status, 500)
                self.assertNotIn("quota-write-fails", body)
                self.assertNotIn(share["token"], body)
            # The rolled-back attempt spent nothing: the share still serves.
            status, body = request(
                base_url, "GET", f"/api/shares/{share['token']}/releases"
            )
            self.assertEqual(status, 200)
            self.assertIn("quota-write-fails", body)
            # And now it really is exhausted.
            status, _ = request(
                base_url, "GET", f"/api/shares/{share['token']}/releases"
            )
            self.assertEqual(status, 429)
            events = list_events(base_url, f"share_id={share['share_id']}&limit=100")[1]
            self.assertEqual(
                [e["outcome"] for e in events],
                ["quota_exhausted", "served"],
            )

    def test_quota_exhausted_filter(self):
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
            self.assertIn("quota_exhausted", SHARE_ACCESS_OUTCOMES)


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
        return create_share(path, **params)

    def test_serve_records_counter_and_event_atomically(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.sqlite3"
            initialize_database(path)
            share = self._new_share(path, max_accesses=2)
            first = serve_share_access(
                path,
                share_id=share["share_id"],
                token_version=1,
                result_count=3,
            )
            self.assertEqual(first["outcome"], "served")
            self.assertEqual(first["result_count"], 3)
            resolved = get_share_by_token(path, share["token"])
            self.assertEqual(resolved["served_count"], 1)
            self.assertEqual(resolved["max_accesses"], 2)
            second = serve_share_access(
                path,
                share_id=share["share_id"],
                token_version=1,
                result_count=1,
            )
            self.assertEqual(second["outcome"], "served")
            third = serve_share_access(
                path,
                share_id=share["share_id"],
                token_version=1,
                result_count=1,
            )
            self.assertEqual(third["outcome"], "quota_exhausted")
            self.assertEqual(third["result_count"], 0)
            resolved = get_share_by_token(path, share["token"])
            self.assertEqual(resolved["served_count"], 2)
            managed = [
                s for s in list_shares(path) if s["share_id"] == share["share_id"]
            ][0]
            self.assertEqual(
                (managed["max_accesses"], managed["served_count"],
                 managed["remaining_accesses"]),
                (2, 2, 0),
            )
            events = list_share_access_events(path, limit=50)
            self.assertEqual(
                [e["outcome"] for e in events],
                ["quota_exhausted", "served", "served"],
            )

    def test_unlimited_storage_never_exhausts(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.sqlite3"
            initialize_database(path)
            share = self._new_share(path)
            for _ in range(5):
                event = serve_share_access(
                    path, share_id=share["share_id"], token_version=1,
                    result_count=0,
                )
                self.assertEqual(event["outcome"], "served")
            resolved = get_share_by_token(path, share["token"])
            self.assertIsNone(resolved["max_accesses"])
            self.assertEqual(resolved["served_count"], 5)
            managed = [
                s for s in list_shares(path) if s["share_id"] == share["share_id"]
            ][0]
            self.assertIsNone(managed["remaining_accesses"])

    def test_concurrent_storage_calls_commit_exactly_max_serves(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.sqlite3"
            initialize_database(path)
            share = self._new_share(path, max_accesses=20)

            def serve(_):
                return serve_share_access(
                    path, share_id=share["share_id"], token_version=1,
                    result_count=1,
                )

            with ThreadPoolExecutor(max_workers=16) as pool:
                events = list(pool.map(serve, range(60)))
            self.assertEqual(sum(e["outcome"] == "served" for e in events), 20)
            self.assertEqual(
                sum(e["outcome"] == "quota_exhausted" for e in events), 40
            )
            with closing(sqlite3.connect(path)) as connection:
                served_count = connection.execute(
                    "SELECT served_count FROM shares WHERE share_id = ?",
                    (share["share_id"],),
                ).fetchone()[0]
                event_count = connection.execute(
                    "SELECT COUNT(*) FROM share_access_events"
                ).fetchone()[0]
            self.assertEqual(served_count, 20)
            self.assertEqual(event_count, 60)

    def test_failed_event_insert_rolls_back_the_counter(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.sqlite3"
            initialize_database(path)
            share = self._new_share(path, max_accesses=3)
            with closing(sqlite3.connect(path)) as connection, connection:
                connection.execute("DROP TABLE share_access_events")
            with self.assertRaises(sqlite3.OperationalError):
                serve_share_access(
                    path, share_id=share["share_id"], token_version=1,
                    result_count=2,
                )
            # Quota untouched by the rolled-back transaction.
            with closing(sqlite3.connect(path)) as connection:
                served_count = connection.execute(
                    "SELECT served_count FROM shares WHERE share_id = ?",
                    (share["share_id"],),
                ).fetchone()[0]
            self.assertEqual(served_count, 0)
            # Recreate the audit table: the share still has all 3 accesses.
            initialize_database(path)
            for _ in range(3):
                event = serve_share_access(
                    path, share_id=share["share_id"], token_version=1,
                    result_count=2,
                )
                self.assertEqual(event["outcome"], "served")
            self.assertEqual(
                serve_share_access(
                    path, share_id=share["share_id"], token_version=1,
                    result_count=2,
                )["outcome"],
                "quota_exhausted",
            )

    def _build_pre_quota_database(self, path: Path, token: str) -> None:
        """Digests-era shares schema without the quota columns."""
        initialize_database(path)
        with closing(sqlite3.connect(path)) as connection, connection:
            connection.execute(
                "CREATE TABLE shares_pre_quota ("
                "share_id TEXT PRIMARY KEY, token_digest TEXT NOT NULL UNIQUE, "
                "dataset_id TEXT NOT NULL, request_id TEXT, "
                "from_time TEXT, to_time TEXT, result_limit INTEGER NOT NULL, "
                "created_at TEXT NOT NULL, expires_at TEXT NOT NULL, revoked_at TEXT)"
            )
            connection.execute(
                "INSERT INTO shares_pre_quota VALUES "
                "('legacy-quota-share', ?, 'retail-demo', 'legacy-quota-req', "
                "NULL, NULL, 7, '2026-01-01T00:00:00+00:00', "
                "'2030-01-01T00:00:00+00:00', '2026-02-01T00:00:00+00:00')",
                (hash_token(token),),
            )
            connection.execute("DROP TABLE shares")
            connection.execute(
                "ALTER TABLE shares_pre_quota RENAME TO shares"
            )
            # Generation backfill happens during initialize, so seed it too.
            connection.execute("DELETE FROM share_tokens")

    def test_legacy_database_migrates_to_unlimited_zero_used(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.sqlite3"
            raw_token = "legacy-quota-token-value"
            self._build_pre_quota_database(path, raw_token)
            initialize_database(path)
            share = get_share_by_token(path, raw_token)
            self.assertIsNotNone(share)
            self.assertEqual(share["share_id"], "legacy-quota-share")
            # Scope, generation and revoked status survive the migration.
            self.assertEqual(share["request_id"], "legacy-quota-req")
            self.assertEqual(share["limit"], 7)
            self.assertEqual(share["token_version"], 1)
            self.assertTrue(share["token_current"])
            self.assertIsNotNone(share["revoked_at"])
            # Quota is backfilled to unlimited / zero used.
            self.assertIsNone(share["max_accesses"])
            self.assertEqual(share["served_count"], 0)
            managed = list_shares(path)
            row = [s for s in managed if s["share_id"] == "legacy-quota-share"][0]
            self.assertIsNone(row["remaining_accesses"])
            self.assertEqual(row["status"], "revoked")
            # Unlimited legacy shares keep serving; idempotent on restart.
            initialize_database(path)
            other = create_share(
                path,
                dataset_id="retail-demo",
                request_id=None,
                start=None,
                end=None,
                limit=5,
                expires_at=datetime.now(timezone.utc) + timedelta(hours=1),
            )
            for _ in range(3):
                event = serve_share_access(
                    path, share_id=other["share_id"], token_version=1,
                    result_count=0,
                )
                self.assertEqual(event["outcome"], "served")
            self.assertIsNone(
                get_share_by_token(path, other["token"])["max_accesses"]
            )

    def test_plaintext_legacy_migration_also_backfills_unlimited_quota(self):
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
                        "legacy-plaintext-1",
                        raw_token,
                        "retail-demo",
                        None,
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
            self.assertEqual(share["share_id"], "legacy-plaintext-1")
            self.assertIsNone(share["max_accesses"])
            self.assertEqual(share["served_count"], 0)
            self.assertEqual(share["limit"], 5)
            with closing(sqlite3.connect(path)) as connection:
                columns = {
                    row[1]
                    for row in connection.execute("PRAGMA table_info(shares)")
                }
            self.assertIn("max_accesses", columns)
            self.assertIn("served_count", columns)


if __name__ == "__main__":
    unittest.main()
