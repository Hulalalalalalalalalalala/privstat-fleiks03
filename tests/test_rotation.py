import hashlib
import json
import re
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

from privstat.database import (
    create_share,
    get_share_by_token,
    hash_token,
    initialize_database,
    is_superseded_token,
    list_shares,
    rotate_share,
)
from privstat.demo import running_demo


def future_iso(**kwargs):
    return (datetime.now(timezone.utc) + timedelta(**kwargs)).isoformat()


def request(base_url, method, path, payload=None):
    data = json.dumps(payload).encode("utf-8") if payload is not None else None
    headers = {"Content-Type": "application/json"} if data is not None else {}
    req = Request(base_url + path, data=data, headers=headers, method=method)
    try:
        with urlopen(req, timeout=10) as response:
            body = response.read().decode("utf-8")
            return response.status, body
    except HTTPError as error:
        body = error.read().decode("utf-8")
        status = error.code
        error.close()
        return status, body


def create_share_http(base_url, **overrides):
    payload = {"dataset_id": "retail-demo", "expires_at": future_iso(hours=1)}
    payload.update(overrides)
    status, body = request(base_url, "POST", "/api/shares", payload)
    assert status == 201, body
    return json.loads(body)


def rotate_http(base_url, share_id, rotation_id="rot-1"):
    return request(
        base_url,
        "POST",
        f"/api/shares/id/{share_id}/rotate",
        {"rotation_id": rotation_id},
    )


class RotateSuccessTests(unittest.TestCase):
    def test_first_rotation_returns_201_with_generation_two(self):
        with running_demo(0) as base_url:
            share = create_share_http(
                base_url, request_id="rot-scope", limit=7
            )
            status, body = rotate_http(base_url, share["share_id"])
            self.assertEqual(status, 201)
            record = json.loads(body)
            self.assertEqual(
                set(record),
                {"share_id", "rotation_id", "token", "token_version", "rotated_at"},
            )
            self.assertEqual(record["share_id"], share["share_id"])
            self.assertEqual(record["rotation_id"], "rot-1")
            self.assertEqual(record["token_version"], 2)
            self.assertGreaterEqual(len(record["token"]), 32)
            self.assertNotEqual(record["token"], share["token"])
            # rotated_at is a UTC microsecond ISO timestamp.
            parsed = datetime.fromisoformat(record["rotated_at"])
            self.assertEqual(parsed.utcoffset(), timedelta(0))
            self.assertRegex(
                record["rotated_at"], r"\.\d{6}\+00:00$"
            )
            # Scope, expiry and share_id are unchanged in the listing.
            status, listing = request(base_url, "GET", "/api/shares")
            self.assertEqual(status, 200)
            row = [s for s in json.loads(listing)
                   if s["share_id"] == share["share_id"]][0]
            self.assertEqual(row["request_id"], "rot-scope")
            self.assertEqual(row["limit"], 7)
            self.assertEqual(row["expires_at"], share["expires_at"])
            self.assertEqual(row["status"], "active")

    def test_each_rotation_increments_the_highest_version(self):
        with running_demo(0) as base_url:
            share = create_share_http(base_url)
            previous_token = share["token"]
            for expected_version in (2, 3, 4):
                status, body = rotate_http(
                    base_url, share["share_id"], f"rot-{expected_version}"
                )
                self.assertEqual(status, 201)
                record = json.loads(body)
                self.assertEqual(record["token_version"], expected_version)
                # Only the newest token works; every older one is gone.
                status, _ = request(
                    base_url, "GET", f"/api/shares/{previous_token}/releases"
                )
                self.assertEqual(status, 410)
                status, _ = request(
                    base_url, "GET", f"/api/shares/{record['token']}/releases"
                )
                self.assertEqual(status, 200)
                previous_token = record["token"]

    def test_rotation_is_read_only_for_budget_members_and_releases(self):
        with running_demo(0) as base_url:
            share = create_share_http(base_url)
            with urlopen(base_url + "/api/privacy-budget", timeout=10) as response:
                budget_before = json.load(response)
            with urlopen(base_url + "/api/releases", timeout=10) as response:
                releases_before = json.load(response)
            status, _ = rotate_http(base_url, share["share_id"])
            self.assertEqual(status, 201)
            with urlopen(base_url + "/api/privacy-budget", timeout=10) as response:
                self.assertEqual(json.load(response), budget_before)
            with urlopen(base_url + "/api/releases", timeout=10) as response:
                self.assertEqual(json.load(response), releases_before)

    def test_token_and_digest_never_appear_in_list_history_or_export(self):
        with running_demo(0) as base_url:
            share = create_share_http(base_url)
            status, body = rotate_http(base_url, share["share_id"])
            self.assertEqual(status, 201)
            new_token = json.loads(body)["token"]
            digests = [
                hash_token(share["token"]),
                hash_token(new_token),
            ]
            for endpoint in (
                "/api/shares",
                "/api/releases",
                "/api/releases/export",
                "/api/releases/export?format=csv",
            ):
                with urlopen(base_url + endpoint, timeout=10) as response:
                    text = response.read().decode("utf-8")
                for secret in (share["token"], new_token, *digests):
                    self.assertNotIn(secret, text)


class RotateValidationTests(unittest.TestCase):
    def test_unknown_share_id_returns_404(self):
        with running_demo(0) as base_url:
            status, _ = rotate_http(base_url, "no-such-share")
            self.assertEqual(status, 404)

    def test_invalid_rotation_id_returns_422(self):
        with running_demo(0) as base_url:
            share = create_share_http(base_url)
            for payload in (
                {},
                {"rotation_id": ""},
                {"rotation_id": "   "},
                {"rotation_id": None},
                {"rotation_id": 42},
                {"rotation_id": ["rot"]},
            ):
                with self.subTest(payload=payload):
                    status, _ = request(
                        base_url,
                        "POST",
                        f"/api/shares/id/{share['share_id']}/rotate",
                        payload,
                    )
                    self.assertEqual(status, 422)
            # The share is untouched: its original token still works.
            status, _ = request(
                base_url, "GET", f"/api/shares/{share['token']}/releases"
            )
            self.assertEqual(status, 200)


class RotateConflictTests(unittest.TestCase):
    def test_revoked_share_returns_409_and_state_is_unchanged(self):
        with running_demo(0) as base_url:
            share = create_share_http(base_url)
            status, _ = request(
                base_url, "DELETE", f"/api/shares/id/{share['share_id']}"
            )
            self.assertEqual(status, 204)
            status, _ = rotate_http(base_url, share["share_id"], "rot-late")
            self.assertEqual(status, 409)
            # No generation was recorded: the same rotation_id is not
            # treated as successful afterwards, and the share stays revoked.
            status, _ = rotate_http(base_url, share["share_id"], "rot-late")
            self.assertEqual(status, 409)
            status, listing = request(
                base_url, "GET", "/api/shares?status=revoked"
            )
            self.assertIn(
                share["share_id"],
                [s["share_id"] for s in json.loads(listing)],
            )

    def test_expired_share_returns_409_and_state_is_unchanged(self):
        with running_demo(0) as base_url:
            share = create_share_http(
                base_url, expires_at=future_iso(seconds=1)
            )
            time.sleep(1.2)
            status, _ = rotate_http(base_url, share["share_id"], "rot-expired")
            self.assertEqual(status, 409)
            status, _ = rotate_http(base_url, share["share_id"], "rot-expired")
            self.assertEqual(status, 409)
            status, listing = request(
                base_url, "GET", "/api/shares?status=expired"
            )
            self.assertIn(
                share["share_id"],
                [s["share_id"] for s in json.loads(listing)],
            )


class RotateIdempotencyTests(unittest.TestCase):
    def test_replay_returns_200_without_token_and_keeps_state(self):
        with running_demo(0) as base_url:
            share = create_share_http(base_url)
            status, body = rotate_http(base_url, share["share_id"], "rot-idem")
            self.assertEqual(status, 201)
            created = json.loads(body)
            status, body = rotate_http(base_url, share["share_id"], "rot-idem")
            self.assertEqual(status, 200)
            replayed = json.loads(body)
            self.assertEqual(
                set(replayed),
                {"share_id", "rotation_id", "token_version", "rotated_at"},
            )
            self.assertNotIn("token", replayed)
            self.assertEqual(
                replayed,
                {key: created[key] for key in replayed},
            )
            # Still generation 2: the replay did not create a new one.
            status, body = rotate_http(base_url, share["share_id"], "rot-next")
            self.assertEqual(status, 201)
            self.assertEqual(json.loads(body)["token_version"], 3)

    def test_replay_takes_precedence_over_expiry_and_revocation(self):
        with running_demo(0) as base_url:
            share = create_share_http(
                base_url, expires_at=future_iso(seconds=1.5)
            )
            status, body = rotate_http(base_url, share["share_id"], "rot-early")
            self.assertEqual(status, 201)
            created = json.loads(body)
            time.sleep(1.7)
            # Expired now, but the successful rotation_id still replays.
            status, body = rotate_http(base_url, share["share_id"], "rot-early")
            self.assertEqual(status, 200)
            self.assertEqual(json.loads(body)["rotated_at"], created["rotated_at"])
            # A new rotation_id on the expired share is still a conflict.
            status, _ = rotate_http(base_url, share["share_id"], "rot-other")
            self.assertEqual(status, 409)

        with running_demo(0) as base_url:
            share = create_share_http(base_url)
            status, _ = rotate_http(base_url, share["share_id"], "rot-early")
            self.assertEqual(status, 201)
            status, _ = request(
                base_url, "DELETE", f"/api/shares/id/{share['share_id']}"
            )
            self.assertEqual(status, 204)
            # Revoked now, but the successful rotation_id still replays.
            status, body = rotate_http(base_url, share["share_id"], "rot-early")
            self.assertEqual(status, 200)
            self.assertNotIn("token", json.loads(body))
            status, _ = rotate_http(base_url, share["share_id"], "rot-other")
            self.assertEqual(status, 409)

    def test_concurrent_same_rotation_id_creates_exactly_one_generation(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.sqlite3"
            initialize_database(path)
            share = create_share(
                path,
                dataset_id="retail-demo",
                request_id=None,
                start=None,
                end=None,
                limit=10,
                expires_at=datetime.now(timezone.utc) + timedelta(hours=1),
            )
            with ThreadPoolExecutor(max_workers=8) as pool:
                results = list(pool.map(
                    lambda _: rotate_share(
                        path, share_id=share["share_id"], rotation_id="rot-race"
                    ),
                    range(8),
                ))
            created = [record for record, was_created in results if was_created]
            replayed = [record for record, was_created in results if not was_created]
            self.assertEqual(len(created), 1)
            self.assertEqual(len(replayed), 7)
            self.assertEqual(created[0]["token_version"], 2)
            for record in replayed:
                self.assertNotIn("token", record)
                self.assertEqual(record["token_version"], 2)
            with closing(sqlite3.connect(path)) as connection:
                rows = connection.execute(
                    "SELECT token_digest, token_version FROM share_rotations"
                ).fetchall()
                (current_digest,) = connection.execute(
                    "SELECT token_digest FROM shares WHERE share_id = ?",
                    (share["share_id"],),
                ).fetchone()
            self.assertEqual(len(rows), 1)
            # Exactly one generation is valid: the stored current digest.
            self.assertEqual(rows[0][1], 2)
            self.assertEqual(rows[0][0], current_digest)
            self.assertEqual(
                current_digest, hash_token(created[0]["token"])
            )


class RotateAccessTests(unittest.TestCase):
    def test_old_token_gets_410_and_unknown_token_still_404(self):
        with running_demo(0) as base_url:
            share = create_share_http(base_url)
            status, _ = rotate_http(base_url, share["share_id"])
            self.assertEqual(status, 201)
            status, _ = request(
                base_url, "GET", f"/api/shares/{share['token']}/releases"
            )
            self.assertEqual(status, 410)
            status, _ = request(
                base_url, "GET", "/api/shares/never-existed/releases"
            )
            self.assertEqual(status, 404)

    def test_revoking_old_token_is_204_and_keeps_current_share_working(self):
        with running_demo(0) as base_url:
            share = create_share_http(base_url)
            status, body = rotate_http(base_url, share["share_id"])
            self.assertEqual(status, 201)
            new_token = json.loads(body)["token"]
            status, _ = request(
                base_url, "DELETE", f"/api/shares/{share['token']}"
            )
            self.assertEqual(status, 204)
            # The share itself was not revoked.
            status, _ = request(
                base_url, "GET", f"/api/shares/{new_token}/releases"
            )
            self.assertEqual(status, 200)
            status, listing = request(base_url, "GET", "/api/shares")
            row = [s for s in json.loads(listing)
                   if s["share_id"] == share["share_id"]][0]
            self.assertEqual(row["status"], "active")
            self.assertIsNone(row["revoked_at"])

    def test_current_token_and_share_id_revocation_keep_original_semantics(self):
        with running_demo(0) as base_url:
            first = create_share_http(base_url)
            status, body = rotate_http(base_url, first["share_id"])
            self.assertEqual(status, 201)
            current = json.loads(body)["token"]
            # Revoking the current token revokes the share.
            status, _ = request(base_url, "DELETE", f"/api/shares/{current}")
            self.assertEqual(status, 204)
            status, _ = request(
                base_url, "GET", f"/api/shares/{current}/releases"
            )
            self.assertEqual(status, 410)

            second = create_share_http(base_url)
            status, body = rotate_http(base_url, second["share_id"])
            self.assertEqual(status, 201)
            current = json.loads(body)["token"]
            # Revoking by share_id still works after a rotation.
            status, _ = request(
                base_url, "DELETE", f"/api/shares/id/{second['share_id']}"
            )
            self.assertEqual(status, 204)
            status, _ = request(
                base_url, "GET", f"/api/shares/{current}/releases"
            )
            self.assertEqual(status, 410)


class RotateStorageTests(unittest.TestCase):
    def test_only_digests_of_every_generation_are_stored(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.sqlite3"
            initialize_database(path)
            share = create_share(
                path,
                dataset_id="retail-demo",
                request_id=None,
                start=None,
                end=None,
                limit=10,
                expires_at=datetime.now(timezone.utc) + timedelta(hours=1),
            )
            record, created = rotate_share(
                path, share_id=share["share_id"], rotation_id="rot-store"
            )
            self.assertTrue(created)
            with closing(sqlite3.connect(path)) as connection:
                rotation_columns = {
                    row[1]
                    for row in connection.execute(
                        "PRAGMA table_info(share_rotations)"
                    )
                }
                self.assertNotIn("token", rotation_columns)
                stored = connection.execute(
                    "SELECT token_digest FROM share_rotations"
                ).fetchall()
                (current,) = connection.execute(
                    "SELECT token_digest FROM shares WHERE share_id = ?",
                    (share["share_id"],),
                ).fetchone()
            self.assertEqual(
                stored, [(hashlib.sha256(
                    record["token"].encode("utf-8")
                ).hexdigest(),)]
            )
            self.assertEqual(current, hash_token(record["token"]))
            # Plaintext tokens never touch the database file.
            raw = path.read_bytes()
            self.assertNotIn(record["token"].encode("utf-8"), raw)
            self.assertNotIn(share["token"].encode("utf-8"), raw)
            # The superseded token is recognized as rotated, not unknown.
            self.assertTrue(is_superseded_token(path, share["token"]))
            self.assertFalse(is_superseded_token(path, record["token"]))
            self.assertFalse(is_superseded_token(path, "never-existed"))
            self.assertIsNone(get_share_by_token(path, share["token"]))
            self.assertIsNotNone(get_share_by_token(path, record["token"]))

    def test_generations_idempotency_and_invalidation_survive_restart(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.sqlite3"
            initialize_database(path)
            share = create_share(
                path,
                dataset_id="retail-demo",
                request_id=None,
                start=None,
                end=None,
                limit=10,
                expires_at=datetime.now(timezone.utc) + timedelta(hours=1),
            )
            record, _ = rotate_share(
                path, share_id=share["share_id"], rotation_id="rot-boot"
            )
            first_rotation = record
            # Simulate restarts: re-running initialization must not lose
            # generations, idempotency records, or invalidation state.
            for iteration in range(2):
                initialize_database(path)
                replayed, created = rotate_share(
                    path, share_id=share["share_id"], rotation_id="rot-boot"
                )
                self.assertFalse(created)
                self.assertEqual(replayed["token_version"], 2)
                self.assertEqual(
                    replayed["rotated_at"], first_rotation["rotated_at"]
                )
                self.assertIsNone(get_share_by_token(path, share["token"]))
                self.assertIsNotNone(get_share_by_token(path, record["token"]))
                self.assertTrue(is_superseded_token(path, share["token"]))
                # The next rotation still builds on the persisted version.
                follow_up, created = rotate_share(
                    path,
                    share_id=share["share_id"],
                    rotation_id=f"rot-boot-next-{iteration}",
                )
                self.assertTrue(created)
                self.assertEqual(follow_up["token_version"], 3 + iteration)
                self.assertTrue(is_superseded_token(path, record["token"]))
                record = follow_up

    def test_list_shares_never_contains_rotation_tokens_or_digests(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.sqlite3"
            initialize_database(path)
            share = create_share(
                path,
                dataset_id="retail-demo",
                request_id=None,
                start=None,
                end=None,
                limit=10,
                expires_at=datetime.now(timezone.utc) + timedelta(hours=1),
            )
            record, _ = rotate_share(
                path, share_id=share["share_id"], rotation_id="rot-list"
            )
            listing = list_shares(path)
            self.assertEqual(len(listing), 1)
            self.assertNotIn("token", listing[0])
            self.assertNotIn("token_digest", listing[0])
            self.assertNotIn("token_version", listing[0])
            serialized = json.dumps(listing)
            self.assertNotIn(record["token"], serialized)
            self.assertNotIn(hash_token(record["token"]), serialized)


class RotateHttpConcurrencyTests(unittest.TestCase):
    def test_concurrent_distinct_rotation_ids_leave_one_valid_token(self):
        with running_demo(0) as base_url:
            share = create_share_http(base_url)
            barrier = threading.Barrier(4)

            def rotate(index):
                barrier.wait(timeout=10)
                return rotate_http(base_url, share["share_id"], f"rot-c{index}")

            with ThreadPoolExecutor(max_workers=4) as pool:
                responses = list(pool.map(rotate, range(4)))
            statuses = sorted(status for status, _ in responses)
            self.assertEqual(statuses, [201, 201, 201, 201])
            versions = sorted(
                json.loads(body)["token_version"]
                for status, body in responses
                if status == 201
            )
            # Each rotation builds on the highest persisted version.
            self.assertEqual(versions, [2, 3, 4, 5])
            newest_token = [
                json.loads(body)["token"]
                for status, body in responses
                if status == 201
                and json.loads(body)["token_version"] == 5
            ][0]
            for status, body in responses:
                if status != 201:
                    continue
                token = json.loads(body)["token"]
                expected = 200 if token == newest_token else 410
                got, _ = request(
                    base_url, "GET", f"/api/shares/{token}/releases"
                )
                self.assertEqual(got, expected)


if __name__ == "__main__":
    unittest.main()
