import json
import secrets
import sqlite3
import socket
import tempfile
import threading
import time
import unittest
from contextlib import closing, contextmanager
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import uvicorn

from privstat.app import create_app
from privstat.database import (
    DATASET_ID,
    create_share,
    get_share_by_token,
    initialize_database,
    revoke_share,
)
from privstat.demo import running_demo


RELEASE_FIELDS = {
    "release_id",
    "request_id",
    "dataset_id",
    "filters",
    "epsilon",
    "published_count",
    "remaining_budget",
    "created_at",
}


def request_json(method, url, payload=None):
    request = Request(
        url,
        data=json.dumps(payload).encode("utf-8") if payload is not None else None,
        headers={"Content-Type": "application/json"},
        method=method,
    )
    try:
        with urlopen(request, timeout=10) as response:
            body = response.read().decode("utf-8")
            return response.status, json.loads(body) if body else None
    except HTTPError as error:
        body = error.read().decode("utf-8")
        status = error.code
        error.close()
        try:
            return status, json.loads(body)
        except ValueError:
            return status, body


def get_json(url):
    with urlopen(url, timeout=10) as response:
        return response.status, json.loads(response.read())


def publish(base_url, request_id, *, epsilon=0.2, filters=None):
    status, record = request_json(
        "POST",
        base_url + "/api/releases",
        {
            "request_id": request_id,
            "dataset_id": DATASET_ID,
            "filters": filters,
            "epsilon": epsilon,
        },
    )
    assert status in (200, 201)
    return record


def create_share_http(base_url, **overrides):
    payload = {"dataset_id": DATASET_ID, "expires_at": "2030-01-01T00:00:00Z"}
    payload.update(overrides)
    return request_json("POST", base_url + "/api/shares", payload)


def share_releases_url(base_url, token, **query):
    suffix = "/api/shares/" + token + "/releases"
    if query:
        suffix += "?" + urlencode(query)
    return request_json("GET", base_url + suffix)


@contextmanager
def running_app_at(database_path: Path):
    """A server backed by an explicit database file, for restart tests."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(("127.0.0.1", 0))
        listener.listen(128)
        server = uvicorn.Server(
            uvicorn.Config(create_app(database_path), log_level="error")
        )
        worker = threading.Thread(target=server.run, kwargs={"sockets": [listener]})
        worker.start()
        try:
            deadline = time.monotonic() + 15
            while not server.started:
                if not worker.is_alive() or time.monotonic() >= deadline:
                    raise RuntimeError("PrivStat service did not start.")
                time.sleep(0.02)
            yield f"http://127.0.0.1:{listener.getsockname()[1]}"
        finally:
            server.should_exit = True
            worker.join(timeout=10)


class CreateShareTests(unittest.TestCase):
    def test_valid_share_returns_201_with_unguessable_ids_and_normalized_scope(self):
        with running_demo(0) as base_url:
            status, body = create_share_http(
                base_url,
                request_id="req-shared",
                **{"from": "2026-01-01T08:00:00+08:00"},
                to="2027-06-01T00:30:00+09:00",
                limit=25,
            )
            self.assertEqual(status, 201)
            self.assertRegex(body["share_id"], r"^[0-9a-f]{32}$")
            self.assertGreaterEqual(len(body["token"]), 32)
            self.assertNotIn(body["token"], body["share_id"])
            self.assertEqual(body["dataset_id"], DATASET_ID)
            self.assertEqual(body["request_id"], "req-shared")
            self.assertEqual(body["limit"], 25)
            # Scope and timestamps are normalized to UTC ISO-8601.
            self.assertEqual(body["from"], "2026-01-01T00:00:00Z")
            self.assertEqual(body["to"], "2027-05-31T15:30:00Z")
            self.assertTrue(body["created_at"])
            self.assertTrue(body["expires_at"].endswith("Z") or "+00:00" in body["expires_at"])

            status, other = create_share_http(base_url)
            self.assertEqual(status, 201)
            self.assertNotEqual(other["share_id"], body["share_id"])
            self.assertNotEqual(other["token"], body["token"])

    def test_defaults_limit_to_50_and_optional_scope_to_null(self):
        with running_demo(0) as base_url:
            status, body = create_share_http(base_url)
            self.assertEqual(status, 201)
            self.assertEqual(body["limit"], 50)
            self.assertIsNone(body["request_id"])
            self.assertIsNone(body["from"])
            self.assertIsNone(body["to"])

    def test_invalid_inputs_return_422(self):
        with running_demo(0) as base_url:
            # Missing required expires_at.
            self.assertEqual(
                request_json(
                    "POST", base_url + "/api/shares", {"dataset_id": DATASET_ID}
                )[0],
                422,
            )
            cases = [
                {"expires_at": "2030-01-01T00:00:00"},  # naive, no timezone
                {"expires_at": "not-a-time"},
                {"expires_at": "2000-01-01T00:00:00Z"},  # in the past
                {"limit": 0, "expires_at": "2030-01-01T00:00:00Z"},
                {"limit": 101, "expires_at": "2030-01-01T00:00:00Z"},
                {"limit": "abc", "expires_at": "2030-01-01T00:00:00Z"},
                {"request_id": "  ", "expires_at": "2030-01-01T00:00:00Z"},
                {"request_id": "", "expires_at": "2030-01-01T00:00:00Z"},
                {"dataset_id": "", "expires_at": "2030-01-01T00:00:00Z"},
                {"dataset_id": "other", "expires_at": "2030-01-01T00:00:00Z"},
            ]
            for overrides in cases:
                payload = {"dataset_id": DATASET_ID, "expires_at": "2030-01-01T00:00:00Z"}
                payload.update(overrides)
                with self.subTest(payload=payload):
                    status, _ = request_json("POST", base_url + "/api/shares", payload)
                    self.assertEqual(status, 422)
            # Naive from/to and inverted windows.
            for payload in (
                {"dataset_id": DATASET_ID, "from": "2026-01-01T00:00:00",
                 "expires_at": "2030-01-01T00:00:00Z"},
                {"dataset_id": DATASET_ID, "to": "2026-01-01T00:00:00",
                 "expires_at": "2030-01-01T00:00:00Z"},
                {"dataset_id": DATASET_ID,
                 "from": "2027-01-01T00:00:00Z", "to": "2026-01-01T00:00:00Z",
                 "expires_at": "2030-01-01T00:00:00Z"},
                {"dataset_id": DATASET_ID,
                 "from": "2026-01-01T00:00:00Z", "to": "2026-01-01T00:00:00Z",
                 "expires_at": "2030-01-01T00:00:00Z"},
            ):
                with self.subTest(payload=payload):
                    self.assertEqual(
                        request_json("POST", base_url + "/api/shares", payload)[0], 422
                    )


class ShareReleasesTests(unittest.TestCase):
    def test_returns_only_in_scope_records_newest_first_with_limit(self):
        with running_demo(0) as base_url:
            first = publish(base_url, "share-win-1")
            time.sleep(0.02)
            second = publish(base_url, "share-win-2")
            time.sleep(0.02)
            third = publish(base_url, "share-win-3")
            status, share = create_share_http(
                base_url,
                **{"from": second["created_at"]},
                to=third["created_at"],
                limit=10,
            )
            self.assertEqual(status, 201)
            status, records = share_releases_url(base_url, share["token"])
            self.assertEqual(status, 200)
            self.assertEqual([r["request_id"] for r in records], ["share-win-2"])
            for record in records:
                self.assertEqual(set(record), RELEASE_FIELDS)
            serialized = json.dumps(records)
            self.assertNotIn("member_id", serialized)
            self.assertNotIn("S001", serialized)
            self.assertNotIn("true_count", serialized)

            # Unbounded share sees everything, limit caps the newest rows.
            status, wide = create_share_http(base_url, limit=2)
            self.assertEqual(status, 201)
            status, records = share_releases_url(base_url, wide["token"])
            self.assertEqual(status, 200)
            self.assertEqual(
                [r["request_id"] for r in records], ["share-win-3", "share-win-2"]
            )

            # request_id scope is an exact filter.
            status, scoped = create_share_http(
                base_url, request_id="share-win-1"
            )
            status, records = share_releases_url(base_url, scoped["token"])
            self.assertEqual([r["request_id"] for r in records], ["share-win-1"])

            # Unknown request_id is an empty success, not an error.
            status, missing = create_share_http(base_url, request_id="no-such-id")
            status, records = share_releases_url(base_url, missing["token"])
            self.assertEqual(status, 200)
            self.assertEqual(records, [])

    def test_window_is_from_inclusive_to_exclusive(self):
        with running_demo(0) as base_url:
            earlier = publish(base_url, "share-bound-1")
            time.sleep(0.02)
            later = publish(base_url, "share-bound-2")
            status, from_share = create_share_http(
                base_url, **{"from": later["created_at"]}
            )
            status, records = share_releases_url(base_url, from_share["token"])
            self.assertEqual({r["request_id"] for r in records}, {"share-bound-2"})
            status, to_share = create_share_http(
                base_url, to=later["created_at"]
            )
            status, records = share_releases_url(base_url, to_share["token"])
            self.assertEqual({r["request_id"] for r in records}, {"share-bound-1"})

    def test_unknown_token_404_expired_or_revoked_410(self):
        with running_demo(0) as base_url:
            status, share = create_share_http(base_url)
            self.assertEqual(status, 201)
            self.assertEqual(share_releases_url(base_url, share["token"])[0], 200)
            self.assertEqual(share_releases_url(base_url, "not-a-real-token")[0], 404)

            # Revocation.
            self.assertEqual(
                request_json("DELETE", base_url + "/api/shares/" + share["token"])[0],
                204,
            )
            self.assertEqual(share_releases_url(base_url, share["token"])[0], 410)

            # Expiration: a short-lived share expiring about a second from now.
            future = datetime.fromtimestamp(time.time() + 1, tz=timezone.utc)
            status, short = create_share_http(
                base_url, expires_at=future.isoformat()
            )
            self.assertEqual(status, 201)
            self.assertEqual(share_releases_url(base_url, short["token"])[0], 200)
            deadline = time.monotonic() + 5
            while share_releases_url(base_url, short["token"])[0] == 200:
                self.assertLess(time.monotonic(), deadline)
                time.sleep(0.1)
            self.assertEqual(share_releases_url(base_url, short["token"])[0], 410)
            # DELETE on an expired share still answers 204.
            self.assertEqual(
                request_json("DELETE", base_url + "/api/shares/" + short["token"])[0],
                204,
            )

    def test_read_is_read_only(self):
        with running_demo(0) as base_url:
            publish(base_url, "share-readonly-1")
            status, share = create_share_http(base_url)
            _, budget_before = get_json(base_url + "/api/privacy-budget")
            _, history_before = get_json(base_url + "/api/releases")
            for _ in range(3):
                self.assertEqual(share_releases_url(base_url, share["token"])[0], 200)
            _, budget_after = get_json(base_url + "/api/privacy-budget")
            _, history_after = get_json(base_url + "/api/releases")
            self.assertEqual(budget_before, budget_after)
            self.assertEqual(history_before, history_after)
            # The token never appears in ordinary history or export.
            _, export = get_json(base_url + "/api/releases/export")
            self.assertNotIn(share["token"], json.dumps(history_after))
            self.assertNotIn(share["token"], json.dumps(export))

    def test_share_link_ignores_query_parameters(self):
        with running_demo(0) as base_url:
            publish(base_url, "share-query-1")
            status, share = create_share_http(base_url, limit=1)
            # The share's own scope/limit govern the response; callers cannot
            # widen them through query parameters (no such route parameters).
            status, records = share_releases_url(
                base_url, share["token"], limit=100
            )
            self.assertEqual(status, 200)
            self.assertLessEqual(len(records), 1)


class RevokeShareTests(unittest.TestCase):
    def test_delete_is_idempotent_and_atomic(self):
        with running_demo(0) as base_url:
            status, share = create_share_http(base_url)
            self.assertEqual(status, 201)
            url = base_url + "/api/shares/" + share["token"]
            self.assertEqual(request_json("DELETE", url)[0], 204)
            # Already revoked: still 204.
            self.assertEqual(request_json("DELETE", url)[0], 204)
            # Unknown token: still 204, state indistinguishable from revoked.
            self.assertEqual(
                request_json("DELETE", base_url + "/api/shares/missing-token")[0],
                204,
            )
            self.assertEqual(share_releases_url(base_url, share["token"])[0], 410)

    def test_revocation_persists_across_restart(self):
        with tempfile.TemporaryDirectory() as directory:
            database_path = Path(directory) / "shares.sqlite3"
            with running_app_at(database_path) as base_url:
                status, share = create_share_http(base_url)
                self.assertEqual(status, 201)
                token = share["token"]
                self.assertEqual(
                    request_json("DELETE", base_url + "/api/shares/" + token)[0],
                    204,
                )
            # A fresh process/connection against the same file must honor it.
            with running_app_at(database_path) as base_url:
                self.assertEqual(share_releases_url(base_url, token)[0], 410)
                self.assertEqual(
                    request_json("DELETE", base_url + "/api/shares/" + token)[0],
                    204,
                )


class ShareStorageTests(unittest.TestCase):
    def test_only_token_hash_is_persisted(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "shares.sqlite3"
            initialize_database(path)
            token = secrets.token_urlsafe(32)
            create_share(
                path,
                token=token,
                dataset_id=DATASET_ID,
                request_id="req-db",
                start=datetime(2026, 1, 1, tzinfo=timezone.utc),
                end=datetime(2027, 1, 1, tzinfo=timezone.utc),
                limit=5,
                expires_at=datetime(2030, 1, 1, tzinfo=timezone.utc),
            )
            share = get_share_by_token(path, token)
            self.assertIsNotNone(share)
            self.assertEqual(share["request_id"], "req-db")
            self.assertIsNone(share["revoked_at"])
            self.assertTrue(revoke_share(path, token))
            self.assertIsNotNone(get_share_by_token(path, token)["revoked_at"])
            # Second revoke keeps the original timestamp and still succeeds.
            revoked_at = get_share_by_token(path, token)["revoked_at"]
            self.assertTrue(revoke_share(path, token))
            self.assertEqual(
                get_share_by_token(path, token)["revoked_at"], revoked_at
            )
            self.assertFalse(revoke_share(path, "unknown-token"))
            # The raw token must never appear in storage.
            with closing(sqlite3.connect(path)) as connection:
                rows = connection.execute("SELECT * FROM shares").fetchall()
            self.assertNotIn(token, repr(rows))

    def test_share_survives_reopen(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "shares.sqlite3"
            initialize_database(path)
            token = secrets.token_urlsafe(32)
            create_share(
                path,
                token=token,
                dataset_id=DATASET_ID,
                request_id=None,
                start=None,
                end=None,
                limit=10,
                expires_at=datetime(2030, 1, 1, tzinfo=timezone.utc),
            )
            # Fresh connections simulate a restart; the share is still usable.
            self.assertIsNotNone(get_share_by_token(path, token))


if __name__ == "__main__":
    unittest.main()
