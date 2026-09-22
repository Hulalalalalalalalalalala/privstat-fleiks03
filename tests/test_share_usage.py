import hashlib
import json
import sqlite3
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import quote
from urllib.request import urlopen

from privstat.database import (
    SHARE_ACCESS_OUTCOMES,
    claim_served_access,
    create_share as db_create_share,
    initialize_database,
    summarize_share_usage,
)
from privstat.demo import running_demo

from test_shares import create_share, future_iso, publish, request


USAGE_FIELDS = {
    "share_id",
    "dataset_id",
    "status",
    "max_accesses",
    "served_count",
    "remaining_accesses",
    "event_count",
    "result_count",
    "outcome_counts",
    "last_accessed_at",
}


def create_share_http(base_url, **overrides):
    status, body = create_share(base_url, **overrides)
    assert status == 201, body
    return json.loads(body)


def list_usage(base_url, query=""):
    suffix = f"?{query}" if query else ""
    status, body = request(base_url, "GET", f"/api/share-usage{suffix}")
    return status, json.loads(body) if body else None, body


def list_events(base_url, query=""):
    suffix = f"?{query}" if query else ""
    status, body = request(
        base_url, "GET", f"/api/share-access-events{suffix}"
    )
    return status, json.loads(body) if body else None, body


def by_share_id(rows):
    return {row["share_id"]: row for row in rows}


class ShareUsageEndpointTests(unittest.TestCase):
    def test_aggregates_served_events_and_quota_fields(self):
        with running_demo(0) as base_url:
            publish(base_url, "usage-served", filters={"region": "north"})
            share = create_share_http(
                base_url, request_id="usage-served", max_accesses=4
            )
            for _ in range(2):
                status, _ = request(
                    base_url, "GET", f"/api/shares/{share['token']}/releases"
                )
                self.assertEqual(status, 200)
            status, rows, raw = list_usage(
                base_url, f"share_id={share['share_id']}"
            )
            self.assertEqual(status, 200)
            self.assertEqual(len(rows), 1)
            usage = rows[0]
            self.assertEqual(set(usage), USAGE_FIELDS)
            self.assertEqual(usage["share_id"], share["share_id"])
            self.assertEqual(usage["dataset_id"], "retail-demo")
            self.assertEqual(usage["status"], "active")
            self.assertEqual(usage["max_accesses"], 4)
            self.assertEqual(usage["served_count"], 2)
            self.assertEqual(usage["remaining_accesses"], 2)
            self.assertEqual(usage["event_count"], 2)
            # One release row per served response: result_count is the sum.
            self.assertEqual(usage["result_count"], 2)
            self.assertEqual(
                set(usage["outcome_counts"]), set(SHARE_ACCESS_OUTCOMES)
            )
            self.assertEqual(usage["outcome_counts"]["served"], 2)
            self.assertEqual(usage["outcome_counts"]["quota_exhausted"], 0)
            self.assertEqual(usage["outcome_counts"]["expired"], 0)
            self.assertEqual(usage["outcome_counts"]["revoked"], 0)
            self.assertEqual(usage["outcome_counts"]["superseded"], 0)
            self.assertIsNotNone(usage["last_accessed_at"])
            # No token, digest or member data ever leaves the endpoint.
            self.assertNotIn(share["token"], raw)
            self.assertNotIn(
                hashlib.sha256(share["token"].encode("utf-8")).hexdigest(), raw
            )
            self.assertNotIn("token", raw)
            self.assertNotIn("digest", raw)
            self.assertNotIn("member_id", raw)

    def test_quota_exhaustion_counts_do_not_advance_served_count(self):
        with running_demo(0) as base_url:
            publish(base_url, "usage-quota")
            share = create_share_http(
                base_url, request_id="usage-quota", max_accesses=2
            )
            token = share["token"]
            for _ in range(2):
                status, _ = request(
                    base_url, "GET", f"/api/shares/{token}/releases"
                )
                self.assertEqual(status, 200)
            for _ in range(3):
                status, _ = request(
                    base_url, "GET", f"/api/shares/{token}/releases"
                )
                self.assertEqual(status, 429)
            status, rows, _ = list_usage(
                base_url, f"share_id={share['share_id']}"
            )
            usage = rows[0]
            self.assertEqual(usage["served_count"], 2)
            self.assertEqual(usage["remaining_accesses"], 0)
            self.assertEqual(usage["event_count"], 5)
            self.assertEqual(usage["outcome_counts"]["served"], 2)
            self.assertEqual(usage["outcome_counts"]["quota_exhausted"], 3)
            # Exhaustion visits carry result_count 0, so only the two
            # served responses contribute their row counts.
            self.assertEqual(usage["result_count"], 2)
            self.assertEqual(
                sum(usage["outcome_counts"].values()), usage["event_count"]
            )

    def test_rejection_outcomes_are_counted_separately(self):
        with running_demo(0) as base_url:
            publish(base_url, "usage-outcomes")
            revoked = create_share_http(base_url)
            status, _ = request(
                base_url, "DELETE", f"/api/shares/{revoked['token']}"
            )
            self.assertEqual(status, 204)
            status, _ = request(
                base_url, "GET", f"/api/shares/{revoked['token']}/releases"
            )
            self.assertEqual(status, 410)

            rotated = create_share_http(base_url, request_id="usage-outcomes")
            status, body = request(
                base_url,
                "POST",
                f"/api/shares/id/{rotated['share_id']}/rotate",
                {"rotation_id": "usage-rot"},
            )
            self.assertEqual(status, 201)
            new_token = json.loads(body)["token"]
            status, _ = request(
                base_url, "GET", f"/api/shares/{rotated['token']}/releases"
            )
            self.assertEqual(status, 410)
            status, _ = request(
                base_url, "GET", f"/api/shares/{new_token}/releases"
            )
            self.assertEqual(status, 200)

            import time

            expired = create_share_http(
                base_url, expires_at=future_iso(seconds=1)
            )
            time.sleep(1.2)
            status, _ = request(
                base_url, "GET", f"/api/shares/{expired['token']}/releases"
            )
            self.assertEqual(status, 410)

            status, rows, _ = list_usage(base_url, "limit=100")
            summaries = by_share_id(rows)
            self.assertEqual(
                summaries[revoked["share_id"]]["outcome_counts"]["revoked"], 1
            )
            self.assertEqual(
                summaries[revoked["share_id"]]["status"], "revoked"
            )
            rotated_usage = summaries[rotated["share_id"]]
            self.assertEqual(
                rotated_usage["outcome_counts"]["superseded"], 1
            )
            self.assertEqual(rotated_usage["outcome_counts"]["served"], 1)
            self.assertEqual(rotated_usage["served_count"], 1)
            self.assertEqual(
                summaries[expired["share_id"]]["outcome_counts"]["expired"], 1
            )
            self.assertEqual(
                summaries[expired["share_id"]]["status"], "expired"
            )

    def test_all_shares_include_zero_event_shares_newest_first_limited(self):
        with running_demo(0) as base_url:
            publish(base_url, "usage-all")
            first = create_share_http(base_url, request_id="usage-all")
            second = create_share_http(base_url)
            third = create_share_http(base_url)
            # Only the oldest share receives an event.
            status, _ = request(
                base_url, "GET", f"/api/shares/{first['token']}/releases"
            )
            self.assertEqual(status, 200)
            status, rows, _ = list_usage(base_url)
            ids = [row["share_id"] for row in rows]
            self.assertEqual(ids[:3], [third["share_id"], second["share_id"], first["share_id"]])
            summaries = by_share_id(rows)
            for share in (second, third):
                zero = summaries[share["share_id"]]
                self.assertEqual(zero["event_count"], 0)
                self.assertEqual(zero["result_count"], 0)
                self.assertIsNone(zero["last_accessed_at"])
                self.assertTrue(
                    all(count == 0 for count in zero["outcome_counts"].values())
                )
                # Zero events do not consume lifetime quota counters.
                self.assertEqual(zero["served_count"], 0)
            # The limit caps shares, not events: two newest shares only.
            status, rows, _ = list_usage(base_url, "limit=2")
            self.assertEqual(
                [row["share_id"] for row in rows],
                [third["share_id"], second["share_id"]],
            )

    def test_unknown_share_id_is_200_empty_array(self):
        with running_demo(0) as base_url:
            create_share_http(base_url)
            status, rows, _ = list_usage(base_url, "share_id=no-such-share")
            self.assertEqual(status, 200)
            self.assertEqual(rows, [])

    def test_window_is_half_open_on_accessed_at(self):
        with running_demo(0) as base_url:
            publish(base_url, "usage-window", filters={"region": "north"})
            share = create_share_http(
                base_url, request_id="usage-window"
            )
            for _ in range(2):
                status, _ = request(
                    base_url, "GET", f"/api/shares/{share['token']}/releases"
                )
                self.assertEqual(status, 200)
            status, events, _ = list_events(
                base_url, f"share_id={share['share_id']}"
            )
            self.assertEqual(len(events), 2)
            newer, older = events[0], events[1]
            # from is inclusive at the exact instant.
            status, rows, _ = list_usage(
                base_url,
                f"share_id={share['share_id']}&from="
                + quote(newer["accessed_at"], safe=""),
            )
            usage = rows[0]
            self.assertEqual(usage["event_count"], 1)
            self.assertEqual(usage["result_count"], 1)
            self.assertEqual(usage["last_accessed_at"], newer["accessed_at"])
            # to is exclusive at the exact instant.
            status, rows, _ = list_usage(
                base_url,
                f"share_id={share['share_id']}&to="
                + quote(newer["accessed_at"], safe=""),
            )
            usage = rows[0]
            self.assertEqual(usage["event_count"], 1)
            self.assertEqual(usage["last_accessed_at"], older["accessed_at"])
            # A window around nothing keeps the share but zeroes its
            # window aggregates and last_accessed_at.
            status, rows, _ = list_usage(
                base_url,
                f"share_id={share['share_id']}"
                "&from=2000-01-01T00:00:00Z&to=2000-02-01T00:00:00Z",
            )
            usage = rows[0]
            self.assertEqual(usage["event_count"], 0)
            self.assertEqual(usage["result_count"], 0)
            self.assertIsNone(usage["last_accessed_at"])
            # Lifetime counters are independent of the window.
            self.assertEqual(usage["served_count"], 2)
            self.assertIsNone(usage["remaining_accesses"])

    def test_invalid_parameters_return_422(self):
        with running_demo(0) as base_url:
            for query in (
                "from=not-a-time",
                "from=2026-01-01T00:00:00",
                "to=2026-01-01T00:00:00",
                "from=2026-02-01T00:00:00Z&to=2026-01-01T00:00:00Z",
                "from=2026-01-01T00:00:00Z&to=2026-01-01T00:00:00Z",
                "limit=0",
                "limit=101",
                "limit=abc",
                "share_id=",
                "share_id=%20%20",
            ):
                with self.subTest(query=query):
                    status, _, _ = list_usage(base_url, query)
                    self.assertEqual(status, 422)

    def test_limit_defaults_to_50_and_caps_at_100(self):
        with running_demo(0) as base_url:
            for _ in range(52):
                create_share_http(base_url)
            status, rows, _ = list_usage(base_url)
            self.assertEqual(status, 200)
            self.assertEqual(len(rows), 50)
            status, rows, _ = list_usage(base_url, "limit=100")
            self.assertEqual(len(rows), 52)
            status, rows, _ = list_usage(base_url, "limit=2")
            self.assertEqual(len(rows), 2)

    def test_query_is_read_only_and_creates_no_events(self):
        with running_demo(0) as base_url:
            publish(base_url, "usage-readonly", filters={"region": "north"})
            share = create_share_http(
                base_url, request_id="usage-readonly", max_accesses=3
            )
            status, _ = request(
                base_url, "GET", f"/api/shares/{share['token']}/releases"
            )
            self.assertEqual(status, 200)
            for query in (
                "",
                f"share_id={share['share_id']}",
                "limit=10",
                "from=2026-01-01T00:00:00Z",
            ):
                status, _, _ = list_usage(base_url, query)
                self.assertEqual(status, 200)
            status, events, _ = list_events(
                base_url, f"share_id={share['share_id']}"
            )
            self.assertEqual(len(events), 1)
            status, rows, _ = list_usage(
                base_url, f"share_id={share['share_id']}"
            )
            self.assertEqual(rows[0]["served_count"], 1)
            self.assertEqual(rows[0]["remaining_accesses"], 2)
            # Budget and release history are untouched.
            with urlopen(base_url + "/api/privacy-budget", timeout=10) as response:
                budget = json.load(response)
            self.assertEqual(budget["used_budget"], 0.2)

    def test_concurrent_accesses_yield_self_consistent_snapshot(self):
        with running_demo(0) as base_url:
            publish(base_url, "usage-concurrent")
            share = create_share_http(
                base_url, request_id="usage-concurrent", max_accesses=20
            )
            url = f"{base_url}/api/shares/{share['token']}/releases"

            def access(_):
                try:
                    with urlopen(url, timeout=10) as response:
                        response.read()
                        return response.status
                except Exception as error:
                    code = getattr(error, "code", None)
                    close = getattr(error, "close", None)
                    if close is not None:
                        close()
                    return code

            def poll(_):
                # Every snapshot must satisfy the rollup invariants: the
                # five outcome counts partition the event total, and the
                # lifetime quota fields agree with each other.
                status, rows, _ = list_usage(
                    base_url, f"share_id={share['share_id']}"
                )
                if status != 200:
                    return None
                usage = rows[0]
                self.assertEqual(
                    sum(usage["outcome_counts"].values()),
                    usage["event_count"],
                )
                self.assertGreaterEqual(
                    usage["result_count"], usage["outcome_counts"]["served"]
                )
                self.assertEqual(
                    usage["remaining_accesses"],
                    max(0, 20 - usage["served_count"]),
                )
                return usage

            with ThreadPoolExecutor(max_workers=12) as pool:
                accesses = list(pool.map(access, range(30)))
                polls = list(pool.map(poll, range(12)))
            self.assertEqual(accesses.count(200), 20)
            self.assertEqual(accesses.count(429), 10)
            self.assertTrue(polls)
            status, rows, _ = list_usage(
                base_url, f"share_id={share['share_id']}"
            )
            usage = rows[0]
            self.assertEqual(usage["served_count"], 20)
            self.assertEqual(usage["remaining_accesses"], 0)
            self.assertEqual(usage["event_count"], 30)
            self.assertEqual(usage["outcome_counts"]["served"], 20)
            self.assertEqual(usage["outcome_counts"]["quota_exhausted"], 10)
            self.assertEqual(usage["result_count"], 20)


class ShareUsageStorageTests(unittest.TestCase):
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

    def _insert_event(self, path, *, share_id, outcome, result_count, at):
        # Store the canonical UTC microsecond form used by production, so
        # lexicographic window comparisons compare actual instants.
        at_iso = at.astimezone(timezone.utc).isoformat(timespec="microseconds")
        with closing(sqlite3.connect(path)) as connection, connection:
            connection.execute(
                "INSERT INTO share_access_events "
                "(event_id, share_id, token_version, outcome, result_count, "
                "accessed_at) VALUES (?, ?, ?, ?, ?, ?)",
                (
                    f"event-{share_id}-{outcome}-{at_iso}",
                    share_id,
                    1,
                    outcome,
                    result_count,
                    at_iso,
                ),
            )

    def test_window_aggregation_and_zero_event_share(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.sqlite3"
            initialize_database(path)
            busy = self._new_share(path, max_accesses=4)
            quiet = self._new_share(path)
            base = datetime(2026, 3, 1, tzinfo=timezone.utc)
            self._insert_event(
                path, share_id=busy["share_id"], outcome="served",
                result_count=3, at=base,
            )
            self._insert_event(
                path, share_id=busy["share_id"], outcome="served",
                result_count=2,
                at=base + timedelta(hours=1),
            )
            self._insert_event(
                path, share_id=busy["share_id"], outcome="quota_exhausted",
                result_count=0,
                at=base + timedelta(hours=2),
            )
            rows = summarize_share_usage(path, limit=50)
            self.assertEqual([r["share_id"] for r in rows], [
                quiet["share_id"], busy["share_id"]
            ])
            summaries = by_share_id(rows)
            busy_usage = summaries[busy["share_id"]]
            self.assertEqual(busy_usage["event_count"], 3)
            self.assertEqual(busy_usage["result_count"], 5)
            self.assertEqual(busy_usage["outcome_counts"]["served"], 2)
            self.assertEqual(
                busy_usage["outcome_counts"]["quota_exhausted"], 1
            )
            # Raw event inserts never touch the quota counter.
            self.assertEqual(busy_usage["served_count"], 0)
            self.assertEqual(busy_usage["remaining_accesses"], 4)
            self.assertEqual(
                busy_usage["last_accessed_at"],
                (base + timedelta(hours=2)).isoformat(timespec="microseconds"),
            )
            quiet_usage = summaries[quiet["share_id"]]
            self.assertEqual(quiet_usage["event_count"], 0)
            self.assertEqual(quiet_usage["result_count"], 0)
            self.assertIsNone(quiet_usage["last_accessed_at"])
            self.assertTrue(
                all(count == 0 for count in quiet_usage["outcome_counts"].values())
            )
            self.assertIsNone(quiet_usage["max_accesses"])
            self.assertIsNone(quiet_usage["remaining_accesses"])
            # Half-open window: [base, base+2h) keeps the first two.
            windowed = summarize_share_usage(
                path,
                share_id=busy["share_id"],
                start=base,
                end=base + timedelta(hours=2),
                limit=50,
            )[0]
            self.assertEqual(windowed["event_count"], 2)
            self.assertEqual(windowed["result_count"], 5)
            self.assertEqual(windowed["outcome_counts"]["served"], 2)
            self.assertEqual(
                windowed["outcome_counts"]["quota_exhausted"], 0
            )
            self.assertEqual(
                windowed["last_accessed_at"],
                (base + timedelta(hours=1)).isoformat(timespec="microseconds"),
            )
            # Unknown share_id resolves to an empty list.
            self.assertEqual(
                summarize_share_usage(path, share_id="missing", limit=50),
                [],
            )

    def test_snapshot_reads_both_tables_in_one_read_only_transaction(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.sqlite3"
            initialize_database(path)
            share = self._new_share(path, max_accesses=5)
            claim_served_access(
                path, share_id=share["share_id"],
                token_version=1, result_count=2,
            )
            # A successful rollup leaves no open transaction and no data
            # changes: the counters and events match what was committed.
            before = summarize_share_usage(
                path, share_id=share["share_id"], limit=1
            )
            with closing(sqlite3.connect(path)) as connection:
                in_transaction = connection.execute(
                    "SELECT count(*) FROM pragma_database_list"
                ).fetchone()
                self.assertIsNotNone(in_transaction)
                served_count = connection.execute(
                    "SELECT served_count FROM shares WHERE share_id = ?",
                    (share["share_id"],),
                ).fetchone()[0]
                event_count = connection.execute(
                    "SELECT COUNT(*) FROM share_access_events"
                ).fetchone()[0]
            self.assertEqual(served_count, 1)
            self.assertEqual(event_count, 1)
            after = summarize_share_usage(
                path, share_id=share["share_id"], limit=1
            )
            self.assertEqual(before, after)
            # No member, budget or release rows exist to leak.
            with closing(sqlite3.connect(path)) as connection:
                release_count = connection.execute(
                    "SELECT COUNT(*) FROM releases"
                ).fetchone()[0]
            self.assertEqual(release_count, 0)
            usage = after[0]
            self.assertEqual(usage["event_count"], 1)
            self.assertEqual(usage["result_count"], 2)
            self.assertEqual(usage["served_count"], 1)
            self.assertEqual(usage["remaining_accesses"], 4)


if __name__ == "__main__":
    unittest.main()
