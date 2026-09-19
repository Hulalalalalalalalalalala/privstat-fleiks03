import hashlib
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
from urllib.parse import quote
from urllib.request import urlopen

from privstat.database import (
    SHARE_ACCESS_OUTCOMES,
    initialize_database,
    list_share_access_events,
    record_share_access_event,
)
from privstat.demo import running_demo

from test_shares import create_share, future_iso, publish, request


EVENT_FIELDS = {
    "event_id",
    "share_id",
    "token_version",
    "outcome",
    "result_count",
    "accessed_at",
}


def create_share_http(base_url, **overrides):
    status, body = create_share(base_url, **overrides)
    assert status == 201, body
    return json.loads(body)


def list_events(base_url, query=""):
    suffix = f"?{query}" if query else ""
    status, body = request(base_url, "GET", f"/api/share-access-events{suffix}")
    return status, json.loads(body) if body else None, body


def assert_utc_microseconds(testcase, raw):
    parsed = datetime.fromisoformat(raw)
    testcase.assertEqual(parsed.utcoffset(), timedelta(0))
    testcase.assertIn(".", raw.split("+")[0])


class ShareAccessEventEndpointTests(unittest.TestCase):
    def test_served_access_records_event_with_result_count(self):
        with running_demo(0) as base_url:
            publish(base_url, "audit-served", filters={"region": "north"})
            share = create_share_http(base_url, request_id="audit-served")
            status, body = request(
                base_url, "GET", f"/api/shares/{share['token']}/releases"
            )
            self.assertEqual(status, 200)
            records = json.loads(body)
            self.assertEqual(len(records), 1)
            status, events, raw = list_events(
                base_url, f"share_id={share['share_id']}"
            )
            self.assertEqual(status, 200)
            self.assertEqual(len(events), 1)
            event = events[0]
            self.assertEqual(set(event), EVENT_FIELDS)
            self.assertEqual(event["share_id"], share["share_id"])
            self.assertEqual(event["outcome"], "served")
            self.assertEqual(event["result_count"], 1)
            self.assertEqual(event["token_version"], 1)
            assert_utc_microseconds(self, event["accessed_at"])
            # Neither the raw token nor its digest ever enters the trail.
            self.assertNotIn(share["token"], raw)
            self.assertNotIn(
                hashlib.sha256(share["token"].encode("utf-8")).hexdigest(), raw
            )
            self.assertNotIn("member_id", raw)

    def test_result_count_matches_response_including_empty_and_limited_views(self):
        with running_demo(0) as base_url:
            publish(base_url, "audit-count-1")
            publish(base_url, "audit-count-2")
            # A scope matching nothing is still a successful serve of zero.
            empty = create_share_http(base_url, request_id="audit-missing")
            status, body = request(
                base_url, "GET", f"/api/shares/{empty['token']}/releases"
            )
            self.assertEqual(status, 200)
            self.assertEqual(json.loads(body), [])
            # The share limit caps both the rows and result_count.
            limited = create_share_http(base_url, limit=1)
            status, body = request(
                base_url, "GET", f"/api/shares/{limited['token']}/releases"
            )
            self.assertEqual(status, 200)
            self.assertEqual(len(json.loads(body)), 1)
            status, events, _ = list_events(
                base_url,
                f"share_id={limited['share_id']}&outcome=served",
            )
            self.assertEqual(status, 200)
            self.assertEqual(events[0]["result_count"], 1)
            status, events, _ = list_events(
                base_url, f"share_id={empty['share_id']}"
            )
            self.assertEqual(events[0]["outcome"], "served")
            self.assertEqual(events[0]["result_count"], 0)

    def test_revoked_expired_and_superseded_are_audited_with_zero_count(self):
        with running_demo(0) as base_url:
            publish(base_url, "audit-outcomes")

            revoked = create_share_http(base_url)
            status, _ = request(
                base_url, "DELETE", f"/api/shares/{revoked['token']}"
            )
            self.assertEqual(status, 204)
            status, _ = request(
                base_url, "GET", f"/api/shares/{revoked['token']}/releases"
            )
            self.assertEqual(status, 410)

            rotated = create_share_http(base_url)
            status, body = request(
                base_url,
                "POST",
                f"/api/shares/id/{rotated['share_id']}/rotate",
                {"rotation_id": "audit-rot"},
            )
            self.assertEqual(status, 201)
            new_token = json.loads(body)["token"]
            status, _ = request(
                base_url, "GET", f"/api/shares/{rotated['token']}/releases"
            )
            self.assertEqual(status, 410)
            status, body = request(
                base_url, "GET", f"/api/shares/{new_token}/releases"
            )
            self.assertEqual(status, 200)

            expired = create_share_http(
                base_url, expires_at=future_iso(seconds=1)
            )
            time.sleep(1.2)
            status, _ = request(
                base_url, "GET", f"/api/shares/{expired['token']}/releases"
            )
            self.assertEqual(status, 410)

            def event_for(share_id, outcome):
                status, events, _ = list_events(
                    base_url, f"share_id={share_id}&outcome={outcome}"
                )
                self.assertEqual(status, 200)
                self.assertEqual(len(events), 1, (share_id, outcome))
                return events[0]

            revoked_event = event_for(revoked["share_id"], "revoked")
            self.assertEqual(revoked_event["result_count"], 0)
            self.assertEqual(revoked_event["token_version"], 1)

            old_event = event_for(rotated["share_id"], "superseded")
            self.assertEqual(old_event["result_count"], 0)
            self.assertEqual(old_event["token_version"], 1)
            served_event = event_for(rotated["share_id"], "served")
            self.assertEqual(served_event["token_version"], 2)
            self.assertEqual(served_event["result_count"], 1)

            expired_event = event_for(expired["share_id"], "expired")
            self.assertEqual(expired_event["result_count"], 0)

    def test_revocation_takes_precedence_over_superseded_generation(self):
        with running_demo(0) as base_url:
            publish(base_url, "audit-precedence")
            share = create_share_http(base_url)
            status, body = request(
                base_url,
                "POST",
                f"/api/shares/id/{share['share_id']}/rotate",
                {"rotation_id": "audit-prec-rot"},
            )
            self.assertEqual(status, 201)
            new_token = json.loads(body)["token"]
            status, _ = request(
                base_url, "DELETE", f"/api/shares/id/{share['share_id']}"
            )
            self.assertEqual(status, 204)
            # Both the old generation and the current one are revoked:
            # the old token must not be mislabeled superseded.
            for token in (share["token"], new_token):
                status, _ = request(
                    base_url, "GET", f"/api/shares/{token}/releases"
                )
                self.assertEqual(status, 410)
            status, events, _ = list_events(
                base_url, f"share_id={share['share_id']}&limit=10"
            )
            self.assertEqual(status, 200)
            self.assertEqual({e["outcome"] for e in events}, {"revoked"})

    def test_unknown_token_is_404_and_not_audited(self):
        with running_demo(0) as base_url:
            status, _ = request(
                base_url, "GET", "/api/shares/no-such-token/releases"
            )
            self.assertEqual(status, 404)
            status, events, _ = list_events(base_url)
            self.assertEqual(status, 200)
            self.assertEqual(events, [])

    def test_events_are_newest_first_and_window_is_half_open(self):
        with running_demo(0) as base_url:
            publish(base_url, "audit-window")
            share = create_share_http(base_url, request_id="audit-window")
            first = second = None
            for _ in range(2):
                status, _ = request(
                    base_url, "GET", f"/api/shares/{share['token']}/releases"
                )
                self.assertEqual(status, 200)
            status, events, _ = list_events(
                base_url, f"share_id={share['share_id']}"
            )
            self.assertEqual(len(events), 2)
            self.assertGreater(
                events[0]["accessed_at"], events[1]["accessed_at"]
            )
            second, first = events[0], events[1]
            # from is inclusive at the exact instant.
            status, events, _ = list_events(
                base_url,
                f"share_id={share['share_id']}&from={quote(second['accessed_at'], safe='')}",
            )
            self.assertEqual([e["event_id"] for e in events], [second["event_id"]])
            # to is exclusive at the exact instant.
            status, events, _ = list_events(
                base_url,
                f"share_id={share['share_id']}&to={quote(second['accessed_at'], safe='')}",
            )
            self.assertEqual([e["event_id"] for e in events], [first["event_id"]])
            # Timezone offsets name the same instant; microsecond gaps count.
            cut = datetime.fromisoformat(first["accessed_at"]) + timedelta(
                microseconds=500
            )
            status, events, _ = list_events(
                base_url,
                f"share_id={share['share_id']}&from={quote(cut.isoformat(), safe='')}",
            )
            self.assertEqual([e["event_id"] for e in events], [second["event_id"]])

    def test_filters_outcome_share_id_and_no_match_is_empty_200(self):
        with running_demo(0) as base_url:
            publish(base_url, "audit-filter")
            share = create_share_http(base_url, request_id="audit-filter")
            other = create_share_http(base_url, request_id="audit-filter")
            status, _ = request(
                base_url, "GET", f"/api/shares/{share['token']}/releases"
            )
            self.assertEqual(status, 200)
            status, _ = request(
                base_url, "DELETE", f"/api/shares/{other['token']}"
            )
            status, _ = request(
                base_url, "GET", f"/api/shares/{other['token']}/releases"
            )
            self.assertEqual(status, 410)
            status, events, _ = list_events(base_url, "outcome=served")
            self.assertEqual(status, 200)
            self.assertTrue(events)
            self.assertTrue(all(e["outcome"] == "served" for e in events))
            status, events, _ = list_events(
                base_url, f"share_id={share['share_id']}"
            )
            self.assertEqual({e["share_id"] for e in events}, {share["share_id"]})
            status, events, _ = list_events(
                base_url, "share_id=does-not-exist"
            )
            self.assertEqual(status, 200)
            self.assertEqual(events, [])

    def test_invalid_parameters_return_422(self):
        with running_demo(0) as base_url:
            for query in (
                "outcome=bogus",
                "outcome=SERVED",
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
                    status, _, _ = list_events(base_url, query)
                    self.assertEqual(status, 422)

    def test_limit_defaults_to_50_and_caps_at_100(self):
        with running_demo(0) as base_url:
            publish(base_url, "audit-limit")
            share = create_share_http(base_url, request_id="audit-limit")
            for _ in range(52):
                status, _ = request(
                    base_url, "GET", f"/api/shares/{share['token']}/releases"
                )
                self.assertEqual(status, 200)
            status, events, _ = list_events(
                base_url, f"share_id={share['share_id']}"
            )
            self.assertEqual(status, 200)
            self.assertEqual(len(events), 50)
            status, events, _ = list_events(
                base_url, f"share_id={share['share_id']}&limit=100"
            )
            self.assertEqual(len(events), 52)
            status, events, _ = list_events(
                base_url, f"share_id={share['share_id']}&limit=2"
            )
            self.assertEqual(len(events), 2)

    def test_querying_events_is_read_only_and_creates_no_events(self):
        with running_demo(0) as base_url:
            publish(base_url, "audit-readonly-query")
            share = create_share_http(
                base_url, request_id="audit-readonly-query"
            )
            status, _ = request(
                base_url, "GET", f"/api/shares/{share['token']}/releases"
            )
            self.assertEqual(status, 200)
            for query in (
                "",
                "outcome=served",
                f"share_id={share['share_id']}&limit=10",
            ):
                status, _, _ = list_events(base_url, query)
                self.assertEqual(status, 200)
            status, events, _ = list_events(
                base_url, f"share_id={share['share_id']}"
            )
            self.assertEqual(len(events), 1)

    def test_concurrent_accesses_each_persist_one_event_without_state_change(self):
        with running_demo(0) as base_url:
            publish(base_url, "audit-concurrent")
            share = create_share_http(base_url, request_id="audit-concurrent")

            def access(_):
                from urllib.request import urlopen

                with urlopen(
                    base_url + f"/api/shares/{share['token']}/releases",
                    timeout=10,
                ) as response:
                    return response.status

            with ThreadPoolExecutor(max_workers=8) as pool:
                statuses = list(pool.map(access, range(8)))
            self.assertTrue(all(status == 200 for status in statuses))
            status, events, _ = list_events(
                base_url, f"share_id={share['share_id']}&limit=100"
            )
            self.assertEqual(status, 200)
            self.assertEqual(len(events), 8)
            self.assertEqual(len({e["event_id"] for e in events}), 8)
            self.assertTrue(all(e["outcome"] == "served" for e in events))
            # Budget, release history and the share itself are untouched.
            with urlopen(base_url + "/api/privacy-budget", timeout=10) as response:
                budget = json.load(response)
            self.assertEqual(budget["used_budget"], 0.2)
            with urlopen(base_url + "/api/shares?limit=100", timeout=10) as response:
                managed = json.load(response)
            listed = [s for s in managed if s["share_id"] == share["share_id"]]
            self.assertEqual(listed[0]["status"], "active")

    def test_failed_audit_write_blocks_share_data(self):
        with running_demo(0) as base_url:
            publish(base_url, "audit-write-fails")
            share = create_share_http(base_url, request_id="audit-write-fails")
            with mock.patch(
                "privstat.app.record_share_access_event",
                side_effect=RuntimeError("audit storage unavailable"),
            ):
                status, body = request(
                    base_url, "GET", f"/api/shares/{share['token']}/releases"
                )
                self.assertEqual(status, 500)
                # Served data must not accompany the failed audit.
                self.assertNotIn("audit-write-fails", body)
                self.assertNotIn(share["token"], body)

                status, _ = request(
                    base_url, "DELETE", f"/api/shares/id/{share['share_id']}"
                )
                self.assertEqual(status, 204)
                status, body = request(
                    base_url, "GET", f"/api/shares/{share['token']}/releases"
                )
                self.assertEqual(status, 500)
                self.assertNotIn("分享已撤销", body)
            # Once storage works again, access behaves normally and the
            # failed served attempt left no half-written event.
            status, body = request(
                base_url, "GET", f"/api/shares/{share['token']}/releases"
            )
            self.assertEqual(status, 410)
            status, events, _ = list_events(
                base_url, f"share_id={share['share_id']}"
            )
            self.assertEqual([e["outcome"] for e in events], ["revoked"])


class ShareAccessEventStorageTests(unittest.TestCase):
    def _new_share(self, path):
        from privstat.database import create_share

        return create_share(
            path,
            dataset_id="retail-demo",
            request_id=None,
            start=None,
            end=None,
            limit=10,
            expires_at=datetime.now(timezone.utc) + timedelta(hours=1),
        )

    def test_table_holds_only_event_fields(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.sqlite3"
            initialize_database(path)
            share = self._new_share(path)
            record_share_access_event(
                path,
                share_id=share["share_id"],
                token_version=1,
                outcome="served",
                result_count=3,
            )
            with closing(sqlite3.connect(path)) as connection:
                columns = {
                    row[1]
                    for row in connection.execute(
                        "PRAGMA table_info(share_access_events)"
                    )
                }
                self.assertEqual(columns, EVENT_FIELDS)
                self.assertNotIn("token", columns)
                self.assertNotIn("token_digest", columns)
                indexes = {
                    row[1]
                    for row in connection.execute(
                        "PRAGMA index_list(share_access_events)"
                    )
                }
            self.assertIn("idx_share_access_events_accessed_at", indexes)
            # The raw token never reaches the file through an event write.
            self.assertNotIn(share["token"].encode("utf-8"), path.read_bytes())

    def test_list_filters_window_order_and_limit(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.sqlite3"
            initialize_database(path)
            share = self._new_share(path)
            first = record_share_access_event(
                path,
                share_id=share["share_id"],
                token_version=1,
                outcome="served",
                result_count=1,
            )
            second = record_share_access_event(
                path,
                share_id=share["share_id"],
                token_version=2,
                outcome="superseded",
                result_count=0,
            )
            rows = list_share_access_events(path, limit=50)
            self.assertEqual([r["event_id"] for r in rows], [
                second["event_id"], first["event_id"]
            ])
            self.assertEqual(set(rows[0]), EVENT_FIELDS)
            cut = datetime.fromisoformat(second["accessed_at"])
            self.assertEqual(
                [r["event_id"] for r in list_share_access_events(
                    path, start=cut, limit=50
                )],
                [second["event_id"]],
            )
            self.assertEqual(
                [r["event_id"] for r in list_share_access_events(
                    path, end=cut, limit=50
                )],
                [first["event_id"]],
            )
            self.assertEqual(
                list_share_access_events(path, outcome="served", limit=50)[0][
                    "event_id"
                ],
                first["event_id"],
            )
            self.assertEqual(
                list_share_access_events(
                    path, share_id="other", limit=50
                ),
                [],
            )
            self.assertEqual(
                len(list_share_access_events(path, limit=1)), 1
            )

    def test_events_survive_restart(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.sqlite3"
            initialize_database(path)
            share = self._new_share(path)
            record_share_access_event(
                path,
                share_id=share["share_id"],
                token_version=1,
                outcome="served",
                result_count=2,
            )
            # Simulate a service restart over the same database file.
            initialize_database(path)
            rows = list_share_access_events(path, limit=50)
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["share_id"], share["share_id"])
            self.assertEqual(rows[0]["outcome"], "served")
            self.assertEqual(rows[0]["result_count"], 2)
            self.assertEqual(set(rows[0]), EVENT_FIELDS)

    def test_concurrent_writes_each_commit_one_row(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.sqlite3"
            initialize_database(path)
            share = self._new_share(path)

            def record(index):
                return record_share_access_event(
                    path,
                    share_id=share["share_id"],
                    token_version=1,
                    outcome=SHARE_ACCESS_OUTCOMES[index % 4],
                    result_count=0,
                )

            with ThreadPoolExecutor(max_workers=8) as pool:
                records = list(pool.map(record, range(16)))
            self.assertEqual(len({r["event_id"] for r in records}), 16)
            with closing(sqlite3.connect(path)) as connection:
                count = connection.execute(
                    "SELECT COUNT(*) FROM share_access_events"
                ).fetchone()[0]
            self.assertEqual(count, 16)
            # No share, budget or release state was touched by events.
            with closing(sqlite3.connect(path)) as connection:
                revoked = connection.execute(
                    "SELECT revoked_at FROM shares WHERE share_id = ?",
                    (share["share_id"],),
                ).fetchone()[0]
                release_count = connection.execute(
                    "SELECT COUNT(*) FROM releases"
                ).fetchone()[0]
            self.assertIsNone(revoked)
            self.assertEqual(release_count, 0)


if __name__ == "__main__":
    unittest.main()
