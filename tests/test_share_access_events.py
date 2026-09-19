import json
import tempfile
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlencode

from privstat.database import (
    initialize_database,
    query_share_access_events,
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


def access_share(base_url, token):
    return request(base_url, "GET", f"/api/shares/{token}/releases")


def list_events(base_url, **params):
    query = f"?{urlencode(params)}" if params else ""
    status, body = request(base_url, "GET", f"/api/share-access-events{query}")
    return status, json.loads(body)


def events_for(base_url, share_id):
    status, events = list_events(base_url, share_id=share_id)
    assert status == 200
    return events


class ShareAccessEventTests(unittest.TestCase):
    def test_served_access_records_event_with_result_count(self):
        with running_demo(0) as base_url:
            publish(base_url, "audit-served")
            status, body = create_share(base_url, request_id="audit-served")
            self.assertEqual(status, 201)
            share = json.loads(body)

            status, body = access_share(base_url, share["token"])
            self.assertEqual(status, 200)
            served = json.loads(body)
            self.assertEqual(len(served), 1)

            events = events_for(base_url, share["share_id"])
            self.assertEqual(len(events), 1)
            event = events[0]
            self.assertEqual(set(event), EVENT_FIELDS)
            self.assertEqual(event["outcome"], "served")
            self.assertEqual(event["result_count"], len(served))
            self.assertEqual(event["token_version"], 1)
            # accessed_at is a UTC timestamp with microsecond precision.
            accessed = datetime.fromisoformat(event["accessed_at"])
            self.assertEqual(accessed.utcoffset(), timedelta(0))
            self.assertIn(".", event["accessed_at"].split("+")[0])
            # The event carries neither the raw token nor its digest.
            self.assertNotIn(share["token"], json.dumps(event))

    def test_refused_accesses_record_zero_count_events(self):
        with running_demo(0) as base_url:
            publish(base_url, "audit-refused")
            # Revoked share.
            status, body = create_share(base_url, request_id="audit-refused")
            share = json.loads(body)
            status, _ = request(
                base_url, "DELETE", f"/api/shares/id/{share['share_id']}"
            )
            self.assertEqual(status, 204)
            status, _ = access_share(base_url, share["token"])
            self.assertEqual(status, 410)
            # Expired share.
            status, body = create_share(
                base_url, expires_at=future_iso(seconds=1)
            )
            expiring = json.loads(body)
            time.sleep(1.2)
            status, _ = access_share(base_url, expiring["token"])
            self.assertEqual(status, 410)
            # Superseded token after rotation.
            status, body = create_share(base_url)
            rotated = json.loads(body)
            status, body = request(
                base_url,
                "POST",
                f"/api/shares/id/{rotated['share_id']}/rotate",
                {"rotation_id": "audit-rot-1"},
            )
            self.assertEqual(status, 201)
            new_token = json.loads(body)["token"]
            status, _ = access_share(base_url, rotated["token"])
            self.assertEqual(status, 410)
            status, _ = access_share(base_url, new_token)
            self.assertEqual(status, 200)

            revoked_events = events_for(base_url, share["share_id"])
            self.assertEqual([e["outcome"] for e in revoked_events], ["revoked"])
            self.assertEqual(revoked_events[0]["result_count"], 0)

            expired_events = events_for(base_url, expiring["share_id"])
            self.assertEqual([e["outcome"] for e in expired_events], ["expired"])
            self.assertEqual(expired_events[0]["result_count"], 0)

            rotated_events = events_for(base_url, rotated["share_id"])
            self.assertEqual(
                [e["outcome"] for e in rotated_events], ["served", "superseded"]
            )
            self.assertEqual(rotated_events[1]["result_count"], 0)
            self.assertEqual(rotated_events[1]["token_version"], 1)
            self.assertEqual(rotated_events[0]["token_version"], 2)

    def test_unknown_token_returns_404_without_event(self):
        with running_demo(0) as base_url:
            status, _ = access_share(base_url, "no-such-token")
            self.assertEqual(status, 404)
            status, events = list_events(base_url)
            self.assertEqual(status, 200)
            self.assertEqual(events, [])

    def test_access_does_not_change_share_state_budget_or_history(self):
        with running_demo(0) as base_url:
            publish(base_url, "audit-stable", epsilon=0.3)
            _, budget_before = request(base_url, "GET", "/api/privacy-budget")
            _, releases_before = request(base_url, "GET", "/api/releases")
            _, shares_before = request(base_url, "GET", "/api/shares")
            status, body = create_share(base_url, request_id="audit-stable")
            share = json.loads(body)
            for _ in range(3):
                status, _ = access_share(base_url, share["token"])
                self.assertEqual(status, 200)
            _, budget_after = request(base_url, "GET", "/api/privacy-budget")
            _, releases_after = request(base_url, "GET", "/api/releases")
            _, shares_after = request(base_url, "GET", "/api/shares")
            self.assertEqual(json.loads(budget_before), json.loads(budget_after))
            self.assertEqual(json.loads(releases_before), json.loads(releases_after))
            before = {s["share_id"]: s for s in json.loads(shares_before)}
            after = {s["share_id"]: s for s in json.loads(shares_after)}
            for share_id, record in before.items():
                self.assertEqual(record, after[share_id])
            self.assertEqual(len(events_for(base_url, share["share_id"])), 3)

    def test_concurrent_accesses_each_record_one_event(self):
        with running_demo(0) as base_url:
            publish(base_url, "audit-concurrent")
            status, body = create_share(base_url, request_id="audit-concurrent")
            share = json.loads(body)
            with ThreadPoolExecutor(max_workers=8) as pool:
                statuses = list(
                    pool.map(
                        lambda _: access_share(base_url, share["token"])[0],
                        range(12),
                    )
                )
            self.assertEqual(statuses, [200] * 12)
            events = events_for(base_url, share["share_id"])
            self.assertEqual(len(events), 12)
            self.assertEqual(len({e["event_id"] for e in events}), 12)
            self.assertTrue(all(e["outcome"] == "served" for e in events))


class ShareAccessEventQueryTests(unittest.TestCase):
    def _seed_events(self, base_url):
        publish(base_url, "audit-query")
        shares = {}
        for name in ("alpha", "beta"):
            status, body = create_share(base_url, request_id="audit-query")
            self.assertEqual(status, 201)
            shares[name] = json.loads(body)
        # alpha: two served accesses; beta: revoked then one refused access.
        access_share(base_url, shares["alpha"]["token"])
        access_share(base_url, shares["alpha"]["token"])
        request(base_url, "DELETE", f"/api/shares/id/{shares['beta']['share_id']}")
        access_share(base_url, shares["beta"]["token"])
        return shares

    def test_filters_by_share_id_and_outcome(self):
        with running_demo(0) as base_url:
            shares = self._seed_events(base_url)
            alpha_events = events_for(base_url, shares["alpha"]["share_id"])
            self.assertEqual(len(alpha_events), 2)
            self.assertTrue(all(e["outcome"] == "served" for e in alpha_events))

            status, events = list_events(base_url, outcome="revoked")
            self.assertEqual(status, 200)
            self.assertEqual(len(events), 1)
            self.assertEqual(events[0]["share_id"], shares["beta"]["share_id"])

            status, events = list_events(
                base_url,
                share_id=shares["alpha"]["share_id"],
                outcome="revoked",
            )
            self.assertEqual(status, 200)
            self.assertEqual(events, [])

    def test_results_are_newest_first_and_limited(self):
        with running_demo(0) as base_url:
            shares = self._seed_events(base_url)
            status, events = list_events(base_url)
            self.assertEqual(status, 200)
            self.assertEqual(len(events), 3)
            accessed = [e["accessed_at"] for e in events]
            self.assertEqual(accessed, sorted(accessed, reverse=True))
            status, events = list_events(base_url, limit=1)
            self.assertEqual(status, 200)
            self.assertEqual(len(events), 1)
            self.assertEqual(events[0]["share_id"], shares["beta"]["share_id"])

    def test_time_window_is_from_inclusive_to_exclusive(self):
        with running_demo(0) as base_url:
            shares = self._seed_events(base_url)
            events = events_for(base_url, shares["alpha"]["share_id"])
            self.assertEqual(len(events), 2)
            first = datetime.fromisoformat(events[-1]["accessed_at"])
            second = datetime.fromisoformat(events[0]["accessed_at"])

            # [first, second) contains only the earlier event.
            status, window = list_events(
                base_url,
                share_id=shares["alpha"]["share_id"],
                **{
                    "from": first.isoformat(),
                    "to": second.isoformat(),
                },
            )
            self.assertEqual(status, 200)
            self.assertEqual(len(window), 1)
            self.assertEqual(window[0]["accessed_at"], events[-1]["accessed_at"])

            # An inclusive from bound keeps the boundary event.
            status, window = list_events(
                base_url,
                share_id=shares["alpha"]["share_id"],
                **{"from": second.isoformat()},
            )
            self.assertEqual(status, 200)
            self.assertEqual(len(window), 1)
            self.assertEqual(window[0]["accessed_at"], events[0]["accessed_at"])

    def test_invalid_filters_return_422(self):
        with running_demo(0) as base_url:
            self._seed_events(base_url)
            bad_params = [
                {"outcome": "unknown"},
                {"from": "not-a-time"},
                {"from": "2026-01-01T00:00:00"},  # missing timezone
                {"to": "2026-01-01 10:00:00"},  # missing timezone
                {
                    "from": "2026-06-01T00:00:00Z",
                    "to": "2026-01-01T00:00:00Z",
                },
                {
                    "from": "2026-01-01T00:00:00Z",
                    "to": "2026-01-01T00:00:00Z",
                },
                {"limit": 0},
                {"limit": 101},
                {"limit": "abc"},
            ]
            for params in bad_params:
                with self.subTest(params=params):
                    status, _ = list_events(base_url, **params)
                    self.assertEqual(status, 422)

    def test_query_does_not_record_events(self):
        with running_demo(0) as base_url:
            self._seed_events(base_url)
            status, before = list_events(base_url)
            self.assertEqual(status, 200)
            status, empty = list_events(base_url, outcome="superseded")
            self.assertEqual(status, 200)
            self.assertEqual(empty, [])
            status, after = list_events(base_url)
            self.assertEqual(status, 200)
            self.assertEqual(before, after)


class ShareAccessEventStorageTests(unittest.TestCase):
    def test_events_survive_reopen(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "audit.sqlite3"
            initialize_database(path)
            record_share_access_event(
                path, share_id="share-1", token_version=1,
                outcome="served", result_count=4,
            )
            # Re-initialize (as a restart would) and read through a fresh
            # connection: the event is still there.
            initialize_database(path)
            events = query_share_access_events(path, share_id="share-1")
            self.assertEqual(len(events), 1)
            self.assertEqual(set(events[0]), EVENT_FIELDS)
            self.assertEqual(events[0]["outcome"], "served")
            self.assertEqual(events[0]["result_count"], 4)


if __name__ == "__main__":
    unittest.main()
