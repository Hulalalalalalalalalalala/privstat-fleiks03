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
    record_share_access_event,
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
    "served",
    "expired",
    "revoked",
    "superseded",
    "quota_exhausted",
    "last_accessed_at",
}


def create_share_http(base_url, **overrides):
    status, body = create_share(base_url, **overrides)
    assert status == 201, body
    return json.loads(body)


def usage(base_url, query=""):
    suffix = f"?{query}" if query else ""
    status, body = request(base_url, "GET", f"/api/share-usage{suffix}")
    return status, json.loads(body) if body else None, body


def usage_for(rows, share_id):
    matches = [row for row in rows if row["share_id"] == share_id]
    assert len(matches) == 1, (share_id, rows)
    return matches[0]


class ShareUsageEndpointTests(unittest.TestCase):
    def test_rollup_fields_counts_and_result_count_sum(self):
        with running_demo(0) as base_url:
            publish(base_url, "usage-rollup", filters={"region": "north"})
            share = create_share_http(
                base_url, request_id="usage-rollup", max_accesses=3
            )
            # Three successful serves of one row each, then one exhaustion.
            request(base_url, "GET", f"/api/shares/{share['token']}/releases")
            request(base_url, "GET", f"/api/shares/{share['token']}/releases")
            request(base_url, "GET", f"/api/shares/{share['token']}/releases")
            request(base_url, "GET", f"/api/shares/{share['token']}/releases")
            status, rows, raw = usage(base_url, "limit=100")
            self.assertEqual(status, 200)
            row = usage_for(rows, share["share_id"])
            self.assertEqual(set(row), USAGE_FIELDS)
            self.assertEqual(row["dataset_id"], "retail-demo")
            self.assertEqual(row["status"], "active")
            self.assertEqual(row["max_accesses"], 3)
            self.assertEqual(row["served_count"], 3)
            self.assertEqual(row["remaining_accesses"], 0)
            self.assertEqual(row["event_count"], 4)
            # result_count is the sum of rows returned by the events:
            # 1 + 1 + 1 + 0 (the quota_exhausted visit returned nothing).
            self.assertEqual(row["result_count"], 3)
            self.assertEqual(row["served"], 3)
            self.assertEqual(row["quota_exhausted"], 1)
            self.assertEqual(row["expired"], 0)
            self.assertEqual(row["revoked"], 0)
            self.assertEqual(row["superseded"], 0)
            self.assertIsNotNone(row["last_accessed_at"])
            # The five per-outcome counts add up to the window event total.
            self.assertEqual(
                sum(row[outcome] for outcome in SHARE_ACCESS_OUTCOMES),
                row["event_count"],
            )
            # No credential material, no member data in the raw response.
            self.assertNotIn(share["token"], raw)
            self.assertNotIn(
                hashlib.sha256(share["token"].encode("utf-8")).hexdigest(), raw
            )
            self.assertNotIn("member_id", raw)

    def test_all_five_outcomes_are_counted_separately(self):
        with running_demo(0) as base_url:
            publish(base_url, "usage-outcomes")
            revoked = create_share_http(base_url)
            request(base_url, "DELETE", f"/api/shares/{revoked['token']}")
            request(base_url, "GET", f"/api/shares/{revoked['token']}/releases")

            rotated = create_share_http(base_url)
            status, body = request(
                base_url,
                "POST",
                f"/api/shares/id/{rotated['share_id']}/rotate",
                {"rotation_id": "usage-rot"},
            )
            self.assertEqual(status, 201)
            new_token = json.loads(body)["token"]
            request(base_url, "GET", f"/api/shares/{rotated['token']}/releases")
            request(base_url, "GET", f"/api/shares/{new_token}/releases")

            expired = create_share_http(
                base_url, expires_at=future_iso(seconds=1)
            )
            import time

            time.sleep(1.2)
            request(base_url, "GET", f"/api/shares/{expired['token']}/releases")

            status, rows, _ = usage(base_url, "limit=100")
            self.assertEqual(status, 200)
            revoked_row = usage_for(rows, revoked["share_id"])
            self.assertEqual(revoked_row["status"], "revoked")
            self.assertEqual(revoked_row["revoked"], 1)
            rotated_row = usage_for(rows, rotated["share_id"])
            self.assertEqual(rotated_row["superseded"], 1)
            self.assertEqual(rotated_row["served"], 1)
            expired_row = usage_for(rows, expired["share_id"])
            self.assertEqual(expired_row["status"], "expired")
            self.assertEqual(expired_row["expired"], 1)
            for row in (revoked_row, rotated_row, expired_row):
                self.assertEqual(
                    sum(row[outcome] for outcome in SHARE_ACCESS_OUTCOMES),
                    row["event_count"],
                )
                # Rejections return zero rows, so they add no result_count.
                self.assertGreaterEqual(row["event_count"], 1)

    def test_zero_event_shares_are_listed_with_null_last_access(self):
        with running_demo(0) as base_url:
            quiet = create_share_http(base_url, max_accesses=5)
            active = create_share_http(base_url)
            request(base_url, "GET", f"/api/shares/{active['token']}/releases")
            status, rows, _ = usage(base_url, "limit=100")
            self.assertEqual(status, 200)
            quiet_row = usage_for(rows, quiet["share_id"])
            self.assertEqual(quiet_row["event_count"], 0)
            self.assertEqual(quiet_row["result_count"], 0)
            for outcome in SHARE_ACCESS_OUTCOMES:
                self.assertEqual(quiet_row[outcome], 0)
            self.assertIsNone(quiet_row["last_accessed_at"])
            # The lifetime quota is still reported for a zero-event share.
            self.assertEqual(quiet_row["served_count"], 0)
            self.assertEqual(quiet_row["max_accesses"], 5)
            self.assertEqual(quiet_row["remaining_accesses"], 5)

    def test_rows_are_newest_share_first_and_limited(self):
        with running_demo(0) as base_url:
            first = create_share_http(base_url)
            second = create_share_http(base_url)
            third = create_share_http(base_url)
            status, rows, _ = usage(base_url, "limit=2")
            self.assertEqual(status, 200)
            self.assertEqual([r["share_id"] for r in rows], [
                third["share_id"], second["share_id"]
            ])
            status, rows, _ = usage(base_url, "limit=100")
            self.assertEqual(
                [r["share_id"] for r in rows],
                [third["share_id"], second["share_id"], first["share_id"]],
            )

    def test_limit_defaults_to_50_and_accepts_100(self):
        with running_demo(0) as base_url:
            ids = []
            for _ in range(52):
                ids.append(create_share_http(base_url)["share_id"])
            status, rows, _ = usage(base_url)
            self.assertEqual(status, 200)
            self.assertEqual(len(rows), 50)
            # Newest first: the last-created share is the first row.
            self.assertEqual(rows[0]["share_id"], ids[-1])
            status, rows, _ = usage(base_url, "limit=100")
            self.assertEqual(len(rows), 52)

    def test_unknown_share_id_is_200_empty_array(self):
        with running_demo(0) as base_url:
            create_share_http(base_url)
            status, rows, _ = usage(base_url, "share_id=no-such-share")
            self.assertEqual(status, 200)
            self.assertEqual(rows, [])

    def test_explicit_share_id_returns_only_that_share(self):
        with running_demo(0) as base_url:
            one = create_share_http(base_url)
            create_share_http(base_url)
            status, rows, _ = usage(
                base_url, f"share_id={one['share_id']}"
            )
            self.assertEqual(status, 200)
            self.assertEqual([r["share_id"] for r in rows], [one["share_id"]])

    def test_window_is_half_open_on_accessed_at(self):
        with running_demo(0) as base_url:
            publish(base_url, "usage-window")
            share = create_share_http(
                base_url, request_id="usage-window", max_accesses=10
            )
            request(base_url, "GET", f"/api/shares/{share['token']}/releases")
            request(base_url, "GET", f"/api/shares/{share['token']}/releases")
            status, body = request(
                base_url,
                "GET",
                f"/api/share-access-events?share_id={share['share_id']}&limit=100",
            )
            self.assertEqual(status, 200)
            events = json.loads(body)
            newest, oldest = events[0], events[-1]
            # from inclusive: starting at the older event keeps both.
            status, rows, _ = usage(
                base_url,
                f"share_id={share['share_id']}&from="
                + quote(oldest["accessed_at"], safe=""),
            )
            self.assertEqual(status, 200)
            self.assertEqual(usage_for(rows, share["share_id"])["event_count"], 2)
            # to exclusive: ending exactly at the newer event excludes it.
            status, rows, _ = usage(
                base_url,
                f"share_id={share['share_id']}&to="
                + quote(newest["accessed_at"], safe=""),
            )
            self.assertEqual(usage_for(rows, share["share_id"])["event_count"], 1)
            # A future-only window still lists the share, with zero events.
            status, rows, _ = usage(
                base_url,
                "from=" + quote("2030-01-01T00:00:00Z", safe=""),
            )
            row = usage_for(rows, share["share_id"])
            self.assertEqual(row["event_count"], 0)
            self.assertIsNone(row["last_accessed_at"])
            # Timezone offsets naming the same instant compare equal.
            cut = (datetime.fromisoformat(oldest["accessed_at"])
                   + timedelta(microseconds=500)).isoformat()
            status, rows, _ = usage(
                base_url,
                f"share_id={share['share_id']}&from=" + quote(cut, safe=""),
            )
            self.assertEqual(usage_for(rows, share["share_id"])["event_count"], 1)

    def test_invalid_parameters_return_422(self):
        with running_demo(0) as base_url:
            for query in (
                "share_id=",
                "share_id=%20%20",
                "from=not-a-time",
                "from=2026-01-01T00:00:00",
                "to=2026-01-01T00:00:00",
                "from=2026-02-01T00:00:00Z&to=2026-01-01T00:00:00Z",
                "from=2026-01-01T00:00:00Z&to=2026-01-01T00:00:00Z",
                "limit=0",
                "limit=101",
                "limit=abc",
            ):
                with self.subTest(query=query):
                    status, _, _ = usage(base_url, query)
                    self.assertEqual(status, 422)

    def test_summary_is_read_only_and_creates_no_events(self):
        with running_demo(0) as base_url:
            publish(base_url, "usage-readonly")
            share = create_share_http(
                base_url, request_id="usage-readonly", max_accesses=3
            )
            request(base_url, "GET", f"/api/shares/{share['token']}/releases")

            def counters():
                status, body = request(base_url, "GET", "/api/shares?limit=100")
                assert status == 200
                listed = usage_for(json.loads(body), share["share_id"])
                status, body = request(
                    base_url, "GET",
                    f"/api/share-access-events?share_id={share['share_id']}"
                    "&limit=100",
                )
                events = json.loads(body)
                status, budget_body = request(
                    base_url, "GET", "/api/privacy-budget"
                )
                return (
                    listed["served_count"],
                    listed["remaining_accesses"],
                    len(events),
                    json.loads(budget_body)["used_budget"],
                )

            before = counters()
            for query in (
                "",
                "limit=10",
                f"share_id={share['share_id']}",
                "from=2026-01-01T00:00:00Z",
            ):
                status, _, _ = usage(base_url, query)
                self.assertEqual(status, 200)
            self.assertEqual(counters(), before)

    def test_concurrent_visits_always_leave_a_consistent_snapshot(self):
        with running_demo(0) as base_url:
            publish(base_url, "usage-concurrent")
            share = create_share_http(
                base_url, request_id="usage-concurrent", max_accesses=40
            )
            url = f"{base_url}/api/shares/{share['token']}/releases"
            snapshots = []

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

            def summarize(_):
                status, body = request(
                    base_url, "GET", "/api/share-usage?limit=100"
                )
                if status == 200:
                    rows = json.loads(body)
                    snapshots.append(usage_for(rows, share["share_id"]))
                return status

            with ThreadPoolExecutor(max_workers=8) as pool:
                accesses = list(pool.map(access, range(24)))
                summaries = list(pool.map(summarize, range(24)))
            self.assertTrue(all(code in (200, 429) for code in accesses))
            self.assertTrue(all(code == 200 for code in summaries))
            self.assertTrue(snapshots)
            for row in snapshots:
                # One snapshot can never mix an old served_count with new
                # events: the lifetime counter equals the served total when
                # no window is applied, and the five outcome counts always
                # sum to the event total.
                self.assertEqual(row["served_count"], row["served"])
                self.assertEqual(
                    sum(row[outcome] for outcome in SHARE_ACCESS_OUTCOMES),
                    row["event_count"],
                )
            # After settling, the rollup matches the audit trail exactly.
            status, events_body = request(
                base_url, "GET",
                f"/api/share-access-events?share_id={share['share_id']}"
                "&limit=100",
            )
            events = json.loads(events_body)
            status, rows, _ = usage(base_url, "limit=100")
            row = usage_for(rows, share["share_id"])
            self.assertEqual(row["event_count"], len(events))
            self.assertEqual(
                row["result_count"], sum(e["result_count"] for e in events)
            )
            for outcome in SHARE_ACCESS_OUTCOMES:
                self.assertEqual(
                    row[outcome],
                    sum(e["outcome"] == outcome for e in events),
                )


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

    def test_storage_rollup_window_order_and_limit(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.sqlite3"
            initialize_database(path)
            first = self._new_share(path, max_accesses=3)
            second = self._new_share(path)
            claim_served_access(
                path, share_id=first["share_id"], token_version=1,
                result_count=4,
            )
            claim_served_access(
                path, share_id=first["share_id"], token_version=1,
                result_count=2,
            )
            record_share_access_event(
                path, share_id=first["share_id"], token_version=1,
                outcome="revoked", result_count=0,
            )
            rows = summarize_share_usage(path, limit=50)
            self.assertEqual([r["share_id"] for r in rows], [
                second["share_id"], first["share_id"]
            ])
            self.assertEqual(set(rows[0]), USAGE_FIELDS)
            quiet, busy = rows[0], rows[1]
            self.assertEqual(quiet["event_count"], 0)
            self.assertEqual(quiet["result_count"], 0)
            self.assertIsNone(quiet["last_accessed_at"])
            self.assertIsNone(quiet["remaining_accesses"])
            self.assertEqual(busy["event_count"], 3)
            self.assertEqual(busy["result_count"], 6)
            self.assertEqual(busy["served"], 2)
            self.assertEqual(busy["revoked"], 1)
            self.assertEqual(busy["served_count"], 2)
            self.assertEqual(busy["remaining_accesses"], 1)
            self.assertEqual(len(summarize_share_usage(path, limit=1)), 1)
            self.assertEqual(summarize_share_usage(path, share_id="missing"), [])
            only = summarize_share_usage(
                path, share_id=first["share_id"], limit=50
            )
            self.assertEqual([r["share_id"] for r in only], [first["share_id"]])
            # Window bounds filter the aggregates, not the share list.
            future = datetime.now(timezone.utc) + timedelta(days=1)
            rows = summarize_share_usage(path, start=future, limit=50)
            self.assertTrue(all(r["event_count"] == 0 for r in rows))
            cut = datetime.fromisoformat(busy["last_accessed_at"])
            rows = summarize_share_usage(path, end=cut, limit=50)
            self.assertEqual(usage_for(rows, first["share_id"])["event_count"], 2)

    def test_status_reflects_revoked_and_expired_in_sql(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.sqlite3"
            initialize_database(path)
            active = self._new_share(path)
            expired = self._new_share(
                path,
                expires_at=datetime.now(timezone.utc) - timedelta(hours=1),
            )
            revoked = self._new_share(path)
            with closing(sqlite3.connect(path)) as connection:
                connection.execute(
                    "UPDATE shares SET revoked_at = ? WHERE share_id = ?",
                    ("2026-01-01T00:00:00.000000+00:00", revoked["share_id"]),
                )
                connection.commit()
            rows = {
                r["share_id"]: r
                for r in summarize_share_usage(path, limit=50)
            }
            self.assertEqual(rows[active["share_id"]]["status"], "active")
            self.assertEqual(rows[expired["share_id"]]["status"], "expired")
            self.assertEqual(rows[revoked["share_id"]]["status"], "revoked")

    def test_summary_reads_only_share_tables_and_writes_nothing(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.sqlite3"
            initialize_database(path)
            share = self._new_share(path, max_accesses=2)
            claim_served_access(
                path, share_id=share["share_id"], token_version=1,
                result_count=3,
            )
            # Repeated summaries change nothing.
            for _ in range(3):
                summarize_share_usage(path, limit=50)
            with closing(sqlite3.connect(path)) as connection:
                share_state = connection.execute(
                    "SELECT served_count, max_accesses, revoked_at FROM shares"
                ).fetchone()
                event_count = connection.execute(
                    "SELECT COUNT(*) FROM share_access_events"
                ).fetchone()[0]
                release_count = connection.execute(
                    "SELECT COUNT(*) FROM releases"
                ).fetchone()[0]
            self.assertEqual(share_state[0], 1)
            self.assertEqual(share_state[1], 2)
            self.assertIsNone(share_state[2])
            self.assertEqual(event_count, 1)
            self.assertEqual(release_count, 0)
            # No token or digest appears in a summary record.
            row = summarize_share_usage(path, share_id=share["share_id"])[0]
            self.assertNotIn("token", row)
            self.assertNotIn("token_digest", row)
            self.assertNotIn(share["token"], json.dumps(row))

    def test_summary_runs_while_write_transaction_is_open(self):
        # The summary opens a deferred read-only transaction, so it does not
        # contend with a writer that has not yet committed.
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.sqlite3"
            initialize_database(path)
            share = self._new_share(path)
            writer = sqlite3.connect(path)
            try:
                writer.isolation_level = None
                writer.execute("BEGIN IMMEDIATE")
                writer.execute(
                    "INSERT INTO share_access_events "
                    "(event_id, share_id, token_version, outcome, result_count, "
                    "accessed_at) VALUES (?, ?, ?, ?, ?, ?)",
                    ("pending-event", share["share_id"], 1, "served", 1,
                     "2026-01-01T00:00:00.000000+00:00"),
                )
                # The uncommitted event must not be visible to the summary.
                rows = summarize_share_usage(path, limit=50)
                self.assertEqual(
                    usage_for(rows, share["share_id"])["event_count"], 0
                )
                writer.execute("COMMIT")
            finally:
                writer.close()
            rows = summarize_share_usage(path, limit=50)
            self.assertEqual(
                usage_for(rows, share["share_id"])["event_count"], 1
            )


if __name__ == "__main__":
    unittest.main()
