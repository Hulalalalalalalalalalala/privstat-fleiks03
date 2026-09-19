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
from urllib.request import urlopen

from privstat.database import (
    create_share as db_create_share,
    get_share_by_token,
    hash_token,
    initialize_database,
    list_shares,
    rotate_share,
)
from privstat.demo import running_demo

from test_shares import create_share, future_iso, publish, request


class RotateEndpointTests(unittest.TestCase):
    def test_first_rotation_returns_201_with_version_2_and_scope_unchanged(self):
        with running_demo(0) as base_url:
            publish(base_url, "rot-scope")
            status, body = create_share(
                base_url,
                request_id="rot-scope",
                **{"from": "2026-01-01T00:00:00Z", "to": "2027-01-01T00:00:00Z"},
                limit=7,
            )
            self.assertEqual(status, 201)
            share = json.loads(body)
            status, body = request(
                base_url,
                "POST",
                f"/api/shares/id/{share['share_id']}/rotate",
                {"rotation_id": "rot-1"},
            )
            self.assertEqual(status, 201)
            rotated = json.loads(body)
            self.assertEqual(
                set(rotated),
                {"share_id", "rotation_id", "token", "token_version", "rotated_at"},
            )
            self.assertEqual(rotated["share_id"], share["share_id"])
            self.assertEqual(rotated["rotation_id"], "rot-1")
            self.assertEqual(rotated["token_version"], 2)
            self.assertGreaterEqual(len(rotated["token"]), 32)
            self.assertNotEqual(rotated["token"], share["token"])
            # rotated_at is a UTC timestamp with microsecond precision.
            rotated_at = datetime.fromisoformat(rotated["rotated_at"])
            self.assertEqual(rotated_at.utcoffset(), timedelta(0))
            self.assertIn(".", rotated["rotated_at"].split("+")[0])
            # Scope, expiry and share_id are unchanged.
            status, body = request(base_url, "GET", "/api/shares")
            self.assertEqual(status, 200)
            listed = [s for s in json.loads(body) if s["share_id"] == share["share_id"]]
            self.assertEqual(len(listed), 1)
            for field in ("request_id", "from", "to", "limit", "created_at", "expires_at"):
                self.assertEqual(listed[0][field], share[field])
            self.assertEqual(listed[0]["status"], "active")

    def test_versions_increment_and_only_newest_token_works(self):
        with running_demo(0) as base_url:
            publish(base_url, "rot-versions")
            status, body = create_share(base_url)
            share = json.loads(body)
            tokens = [share["token"]]
            for version, rotation_id in ((2, "rot-a"), (3, "rot-b")):
                status, body = request(
                    base_url,
                    "POST",
                    f"/api/shares/id/{share['share_id']}/rotate",
                    {"rotation_id": rotation_id},
                )
                self.assertEqual(status, 201)
                rotated = json.loads(body)
                self.assertEqual(rotated["token_version"], version)
                tokens.append(rotated["token"])
            # Every superseded generation gets 410, the newest works.
            for old_token in tokens[:-1]:
                status, _ = request(
                    base_url, "GET", f"/api/shares/{old_token}/releases"
                )
                self.assertEqual(status, 410)
            status, body = request(
                base_url, "GET", f"/api/shares/{tokens[-1]}/releases"
            )
            self.assertEqual(status, 200)
            self.assertEqual(
                [r["request_id"] for r in json.loads(body)], ["rot-versions"]
            )

    def test_unknown_share_id_returns_404(self):
        with running_demo(0) as base_url:
            status, _ = request(
                base_url,
                "POST",
                "/api/shares/id/no-such-share/rotate",
                {"rotation_id": "rot-x"},
            )
            self.assertEqual(status, 404)

    def test_invalid_rotation_id_returns_422(self):
        with running_demo(0) as base_url:
            status, body = create_share(base_url)
            share_id = json.loads(body)["share_id"]
            for payload in (
                {},
                {"rotation_id": ""},
                {"rotation_id": "   "},
                {"rotation_id": 123},
                {"rotation_id": None},
                {"rotation_id": "rot-extra", "unexpected": 1},
                {"rotation_id": "rot-extra", "x": {"nested": True}},
            ):
                with self.subTest(payload=payload):
                    status, _ = request(
                        base_url,
                        "POST",
                        f"/api/shares/id/{share_id}/rotate",
                        payload,
                    )
                    self.assertEqual(status, 422)

    def test_revoked_share_returns_409_and_keeps_state(self):
        with running_demo(0) as base_url:
            status, body = create_share(base_url)
            share = json.loads(body)
            status, _ = request(
                base_url, "DELETE", f"/api/shares/id/{share['share_id']}"
            )
            self.assertEqual(status, 204)
            status, _ = request(
                base_url,
                "POST",
                f"/api/shares/id/{share['share_id']}/rotate",
                {"rotation_id": "rot-revoked"},
            )
            self.assertEqual(status, 409)
            # State unchanged: still revoked, original token still 410.
            status, body = request(
                base_url, "GET", "/api/shares?status=revoked"
            )
            self.assertEqual(status, 200)
            self.assertIn(
                share["share_id"], [s["share_id"] for s in json.loads(body)]
            )
            status, _ = request(
                base_url, "GET", f"/api/shares/{share['token']}/releases"
            )
            self.assertEqual(status, 410)

    def test_expired_share_returns_409(self):
        with running_demo(0) as base_url:
            status, body = create_share(
                base_url, expires_at=future_iso(seconds=1)
            )
            self.assertEqual(status, 201)
            share = json.loads(body)
            time.sleep(1.2)
            status, _ = request(
                base_url,
                "POST",
                f"/api/shares/id/{share['share_id']}/rotate",
                {"rotation_id": "rot-expired"},
            )
            self.assertEqual(status, 409)

    def test_replay_returns_200_without_token_and_wins_over_revocation(self):
        with running_demo(0) as base_url:
            status, body = create_share(base_url)
            share = json.loads(body)
            status, body = request(
                base_url,
                "POST",
                f"/api/shares/id/{share['share_id']}/rotate",
                {"rotation_id": "rot-replay"},
            )
            self.assertEqual(status, 201)
            created = json.loads(body)
            # Revoke the share, then replay: the committed rotation_id
            # still replays with 200 and never changes state.
            status, _ = request(
                base_url, "DELETE", f"/api/shares/id/{share['share_id']}"
            )
            self.assertEqual(status, 204)
            for _ in range(2):
                status, body = request(
                    base_url,
                    "POST",
                    f"/api/shares/id/{share['share_id']}/rotate",
                    {"rotation_id": "rot-replay"},
                )
                self.assertEqual(status, 200)
                replayed = json.loads(body)
                self.assertNotIn("token", replayed)
                self.assertEqual(
                    replayed,
                    {
                        "share_id": created["share_id"],
                        "rotation_id": "rot-replay",
                        "token_version": created["token_version"],
                        "rotated_at": created["rotated_at"],
                    },
                )
            # The replay did not resurrect the share.
            status, _ = request(
                base_url, "GET", f"/api/shares/{created['token']}/releases"
            )
            self.assertEqual(status, 410)

    def test_old_token_revoke_is_204_and_keeps_current_share_alive(self):
        with running_demo(0) as base_url:
            status, body = create_share(base_url)
            share = json.loads(body)
            status, body = request(
                base_url,
                "POST",
                f"/api/shares/id/{share['share_id']}/rotate",
                {"rotation_id": "rot-revoke-old"},
            )
            self.assertEqual(status, 201)
            new_token = json.loads(body)["token"]
            # Revoking a superseded token is a harmless idempotent 204.
            status, body = request(
                base_url, "DELETE", f"/api/shares/{share['token']}"
            )
            self.assertEqual(status, 204)
            self.assertEqual(body, "")
            status, body = request(
                base_url, "GET", f"/api/shares/{new_token}/releases"
            )
            self.assertEqual(status, 200)
            # Revoking the current token still revokes the share.
            status, _ = request(base_url, "DELETE", f"/api/shares/{new_token}")
            self.assertEqual(status, 204)
            status, _ = request(
                base_url, "GET", f"/api/shares/{new_token}/releases"
            )
            self.assertEqual(status, 410)

    def test_rotation_has_no_side_effects_and_token_stays_hidden(self):
        with running_demo(0) as base_url:
            publish(base_url, "rot-side-effects")
            status, body = create_share(base_url)
            share = json.loads(body)
            with urlopen(base_url + "/api/privacy-budget", timeout=10) as response:
                budget_before = json.load(response)
            with urlopen(base_url + "/api/releases", timeout=10) as response:
                history_before = json.load(response)
            status, body = request(
                base_url,
                "POST",
                f"/api/shares/id/{share['share_id']}/rotate",
                {"rotation_id": "rot-hidden"},
            )
            self.assertEqual(status, 201)
            new_token = json.loads(body)["token"]
            with urlopen(base_url + "/api/privacy-budget", timeout=10) as response:
                self.assertEqual(json.load(response), budget_before)
            with urlopen(base_url + "/api/releases", timeout=10) as response:
                self.assertEqual(json.load(response), history_before)
            # Neither the new token nor its digest leaks anywhere.
            digest = hashlib.sha256(new_token.encode("utf-8")).hexdigest()
            for endpoint in (
                "/api/releases",
                "/api/releases/export",
                "/api/releases/export?format=csv",
                "/api/shares",
            ):
                with urlopen(base_url + endpoint, timeout=10) as response:
                    text = response.read().decode("utf-8")
                self.assertNotIn(new_token, text)
                self.assertNotIn(digest, text)


class RotateStorageTests(unittest.TestCase):
    def _new_share(self, path):
        return db_create_share(
            path,
            dataset_id="retail-demo",
            request_id=None,
            start=None,
            end=None,
            limit=10,
            expires_at=datetime.now(timezone.utc) + timedelta(hours=1),
        )

    def test_only_digests_of_all_generations_are_stored(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.sqlite3"
            initialize_database(path)
            share = self._new_share(path)
            status, rotated = rotate_share(
                path, share_id=share["share_id"], rotation_id="rot-db"
            )
            self.assertEqual(status, "created")
            with closing(sqlite3.connect(path)) as connection:
                columns = {
                    row[1]
                    for row in connection.execute("PRAGMA table_info(share_tokens)")
                }
                self.assertNotIn("token", columns)
                rows = connection.execute(
                    "SELECT token_version, token_digest, rotation_id "
                    "FROM share_tokens WHERE share_id = ? ORDER BY token_version",
                    (share["share_id"],),
                ).fetchall()
            self.assertEqual(
                rows,
                [
                    (1, hash_token(share["token"]), None),
                    (2, hash_token(rotated["token"]), "rot-db"),
                ],
            )
            # The plaintext of neither generation lingers in the file.
            raw = path.read_bytes()
            self.assertNotIn(share["token"].encode("utf-8"), raw)
            self.assertNotIn(rotated["token"].encode("utf-8"), raw)
            # Resolution reports currency per generation.
            self.assertTrue(get_share_by_token(path, rotated["token"])["token_current"])
            old = get_share_by_token(path, share["token"])
            self.assertIsNotNone(old)
            self.assertFalse(old["token_current"])
            self.assertEqual(old["token_version"], 1)

    def test_concurrent_same_rotation_id_creates_exactly_one_generation(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.sqlite3"
            initialize_database(path)
            share = self._new_share(path)
            with ThreadPoolExecutor(max_workers=8) as pool:
                results = list(
                    pool.map(
                        lambda _: rotate_share(
                            path, share_id=share["share_id"], rotation_id="rot-conc"
                        ),
                        range(8),
                    )
                )
            statuses = [status for status, _ in results]
            self.assertEqual(statuses.count("created"), 1)
            self.assertEqual(statuses.count("replayed"), 7)
            records = [record for _, record in results]
            self.assertEqual({r["token_version"] for r in records}, {2})
            self.assertEqual({r["rotated_at"] for r in records}, {records[0]["rotated_at"]})
            tokens = [r["token"] for r in records if "token" in r]
            self.assertEqual(len(tokens), 1)
            with closing(sqlite3.connect(path)) as connection:
                count = connection.execute(
                    "SELECT COUNT(*) FROM share_tokens WHERE share_id = ?",
                    (share["share_id"],),
                ).fetchone()[0]
            self.assertEqual(count, 2)

    def test_generations_and_idempotency_survive_restart(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.sqlite3"
            initialize_database(path)
            share = self._new_share(path)
            status, rotated = rotate_share(
                path, share_id=share["share_id"], rotation_id="rot-restart"
            )
            self.assertEqual(status, "created")
            # Simulate a restart: re-run initialization on the same file.
            initialize_database(path)
            status, replayed = rotate_share(
                path, share_id=share["share_id"], rotation_id="rot-restart"
            )
            self.assertEqual(status, "replayed")
            self.assertEqual(replayed["token_version"], 2)
            self.assertEqual(replayed["rotated_at"], rotated["rotated_at"])
            self.assertNotIn("token", replayed)
            # Invalidation state persists: old token superseded, new current.
            self.assertFalse(get_share_by_token(path, share["token"])["token_current"])
            self.assertTrue(get_share_by_token(path, rotated["token"])["token_current"])
            # The next rotation continues from the persisted maximum.
            status, third = rotate_share(
                path, share_id=share["share_id"], rotation_id="rot-restart-2"
            )
            self.assertEqual(status, "created")
            self.assertEqual(third["token_version"], 3)

    def test_legacy_database_backfills_generation_one(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.sqlite3"
            initialize_database(path)
            # Simulate a pre-rotation database: shares without token rows.
            share = self._new_share(path)
            with closing(sqlite3.connect(path)) as connection, connection:
                connection.execute("DELETE FROM share_tokens")
            initialize_database(path)
            resolved = get_share_by_token(path, share["token"])
            self.assertIsNotNone(resolved)
            self.assertTrue(resolved["token_current"])
            self.assertEqual(resolved["token_version"], 1)
            # The first rotation after backfill yields version 2.
            status, rotated = rotate_share(
                path, share_id=share["share_id"], rotation_id="rot-legacy"
            )
            self.assertEqual(status, "created")
            self.assertEqual(rotated["token_version"], 2)

    def test_list_shares_never_contains_rotation_data(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.sqlite3"
            initialize_database(path)
            share = self._new_share(path)
            rotate_share(path, share_id=share["share_id"], rotation_id="rot-list")
            for record in list_shares(path):
                self.assertNotIn("token", record)
                self.assertNotIn("token_digest", record)
                self.assertNotIn("token_version", record)
                self.assertNotIn("rotation_id", record)


if __name__ == "__main__":
    unittest.main()
