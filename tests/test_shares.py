import json
import time
import unittest
from datetime import datetime, timedelta, timezone
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from privstat.demo import running_demo


PUBLIC_FIELDS = {
    "release_id",
    "request_id",
    "dataset_id",
    "filters",
    "epsilon",
    "published_count",
    "remaining_budget",
    "created_at",
}


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


def publish(base_url, request_id, *, filters=None, epsilon=0.2):
    status, body = request(
        base_url,
        "POST",
        "/api/releases",
        {
            "request_id": request_id,
            "dataset_id": "retail-demo",
            "filters": filters,
            "epsilon": epsilon,
        },
    )
    assert status in (200, 201)
    return json.loads(body)


def create_share(base_url, **overrides):
    payload = {"dataset_id": "retail-demo", "expires_at": future_iso(hours=1)}
    payload.update(overrides)
    status, body = request(base_url, "POST", "/api/shares", payload)
    return status, body


class ShareCreateTests(unittest.TestCase):
    def test_create_returns_201_with_unguessable_credentials_and_scope(self):
        with running_demo(0) as base_url:
            status, body = create_share(
                base_url,
                request_id="req-1",
                **{"from": "2026-01-01T00:00:00Z", "to": "2027-01-01T00:00:00Z"},
                limit=10,
            )
            self.assertEqual(status, 201)
            share = json.loads(body)
            self.assertEqual(
                set(share),
                {
                    "share_id",
                    "token",
                    "dataset_id",
                    "request_id",
                    "from",
                    "to",
                    "limit",
                    "created_at",
                    "expires_at",
                },
            )
            self.assertGreaterEqual(len(share["token"]), 32)
            self.assertGreaterEqual(len(share["share_id"]), 16)
            self.assertEqual(share["dataset_id"], "retail-demo")
            self.assertEqual(share["request_id"], "req-1")
            self.assertEqual(share["from"], "2026-01-01T00:00:00.000000+00:00")
            self.assertEqual(share["to"], "2027-01-01T00:00:00.000000+00:00")
            self.assertEqual(share["limit"], 10)
            # Tokens are unique and unguessable between shares.
            status, other = create_share(base_url)
            self.assertEqual(status, 201)
            self.assertNotEqual(share["token"], json.loads(other)["token"])
            self.assertNotEqual(share["share_id"], json.loads(other)["share_id"])

    def test_timezone_offsets_are_normalized(self):
        with running_demo(0) as base_url:
            status, body = create_share(
                base_url, **{"from": "2026-06-01T08:00:00+08:00"}
            )
            self.assertEqual(status, 201)
            self.assertEqual(
                json.loads(body)["from"], "2026-06-01T00:00:00.000000+00:00"
            )

    def test_invalid_input_returns_422(self):
        with running_demo(0) as base_url:
            cases = [
                {"dataset_id": "other-dataset"},
                {"dataset_id": "  "},
                {"request_id": "   "},
                {"expires_at": "not-a-time"},
                {"expires_at": "2027-01-01T00:00:00"},  # no timezone
                {"expires_at": "2020-01-01T00:00:00Z"},  # in the past
                {"from": "2026-06-01T00:00:00"},  # no timezone
                {"to": "2026-06-01T00:00:00"},  # no timezone
                {"from": "not-a-time"},
                {"limit": 0},
                {"limit": 101},
                {"limit": "abc"},
                {
                    "from": "2026-06-01T00:00:00Z",
                    "to": "2026-06-01T00:00:00Z",
                },
                {
                    "from": "2026-07-01T00:00:00Z",
                    "to": "2026-06-01T00:00:00Z",
                },
            ]
            for overrides in cases:
                with self.subTest(overrides=overrides):
                    status, _ = create_share(base_url, **overrides)
                    self.assertEqual(status, 422)
            # expires_at is required.
            status, _ = request(
                base_url, "POST", "/api/shares", {"dataset_id": "retail-demo"}
            )
            self.assertEqual(status, 422)


class ShareAccessTests(unittest.TestCase):
    def test_returns_scoped_records_newest_first_with_public_fields(self):
        with running_demo(0) as base_url:
            publish(base_url, "share-a", filters={"region": "north"})
            publish(base_url, "share-b", filters={"region": "south"})
            publish(base_url, "share-c")
            status, body = create_share(base_url, request_id="share-a")
            self.assertEqual(status, 201)
            token = json.loads(body)["token"]
            status, body = request(base_url, "GET", f"/api/shares/{token}/releases")
            self.assertEqual(status, 200)
            records = json.loads(body)
            self.assertEqual([r["request_id"] for r in records], ["share-a"])
            for record in records:
                self.assertEqual(set(record), PUBLIC_FIELDS)
            self.assertNotIn("member_id", body)
            self.assertNotIn("S001", body)
            self.assertNotIn("true_count", body)
            # Unscoped share returns everything, newest first.
            status, body = create_share(base_url, limit=2)
            token = json.loads(body)["token"]
            status, body = request(base_url, "GET", f"/api/shares/{token}/releases")
            self.assertEqual(status, 200)
            records = json.loads(body)
            self.assertEqual(
                [r["request_id"] for r in records], ["share-c", "share-b"]
            )

    def test_time_window_is_from_inclusive_to_exclusive(self):
        with running_demo(0) as base_url:
            earlier = publish(base_url, "share-time-1")
            later = publish(base_url, "share-time-2")
            status, body = create_share(
                base_url,
                **{
                    "from": earlier["created_at"],
                    "to": later["created_at"],
                },
            )
            token = json.loads(body)["token"]
            status, body = request(base_url, "GET", f"/api/shares/{token}/releases")
            self.assertEqual(status, 200)
            self.assertEqual(
                [r["request_id"] for r in json.loads(body)], ["share-time-1"]
            )

    def test_access_is_read_only(self):
        with running_demo(0) as base_url:
            publish(base_url, "share-readonly")
            status, body = create_share(base_url)
            token = json.loads(body)["token"]
            with urlopen(base_url + "/api/privacy-budget", timeout=10) as response:
                budget_before = json.load(response)
            with urlopen(base_url + "/api/releases", timeout=10) as response:
                history_before = json.load(response)
            for _ in range(3):
                status, _ = request(base_url, "GET", f"/api/shares/{token}/releases")
                self.assertEqual(status, 200)
            with urlopen(base_url + "/api/privacy-budget", timeout=10) as response:
                budget_after = json.load(response)
            with urlopen(base_url + "/api/releases", timeout=10) as response:
                history_after = json.load(response)
            self.assertEqual(budget_before, budget_after)
            self.assertEqual(history_before, history_after)

    def test_unknown_token_access_returns_404_but_revoke_is_204(self):
        with running_demo(0) as base_url:
            status, _ = request(
                base_url, "GET", "/api/shares/no-such-token/releases"
            )
            self.assertEqual(status, 404)
            # Revocation by token is unconditionally idempotent, so an
            # unknown token cannot be probed through the DELETE response.
            status, body = request(base_url, "DELETE", "/api/shares/no-such-token")
            self.assertEqual(status, 204)
            self.assertEqual(body, "")

    def test_expired_share_returns_410(self):
        with running_demo(0) as base_url:
            publish(base_url, "share-expired")
            status, body = create_share(
                base_url, expires_at=future_iso(seconds=1)
            )
            self.assertEqual(status, 201)
            token = json.loads(body)["token"]
            time.sleep(1.2)
            status, _ = request(base_url, "GET", f"/api/shares/{token}/releases")
            self.assertEqual(status, 410)

    def test_token_never_appears_in_history_or_export(self):
        with running_demo(0) as base_url:
            publish(base_url, "share-hidden")
            status, body = create_share(base_url)
            token = json.loads(body)["token"]
            share_id = json.loads(body)["share_id"]
            with urlopen(base_url + "/api/releases", timeout=10) as response:
                history = response.read().decode("utf-8")
            with urlopen(base_url + "/api/releases/export", timeout=10) as response:
                exported = response.read().decode("utf-8")
            with urlopen(
                base_url + "/api/releases/export?format=csv", timeout=10
            ) as response:
                exported_csv = response.read().decode("utf-8")
            for text in (history, exported, exported_csv):
                self.assertNotIn(token, text)
                self.assertNotIn(share_id, text)


class ShareRevokeTests(unittest.TestCase):
    def test_revoke_is_idempotent_and_blocks_access(self):
        with running_demo(0) as base_url:
            publish(base_url, "share-revoke")
            status, body = create_share(base_url)
            token = json.loads(body)["token"]
            status, body = request(base_url, "GET", f"/api/shares/{token}/releases")
            self.assertEqual(status, 200)
            status, body = request(base_url, "DELETE", f"/api/shares/{token}")
            self.assertEqual(status, 204)
            self.assertEqual(body, "")
            # Idempotent: revoking again still returns 204.
            status, _ = request(base_url, "DELETE", f"/api/shares/{token}")
            self.assertEqual(status, 204)
            # Access after the revoke committed must not succeed.
            status, _ = request(base_url, "GET", f"/api/shares/{token}/releases")
            self.assertEqual(status, 410)


class ExportPrecisionTests(unittest.TestCase):
    def test_time_bounds_are_not_truncated_to_milliseconds(self):
        with running_demo(0) as base_url:
            record = publish(base_url, "exp-precision")
            created = datetime.fromisoformat(record["created_at"])

            def export(**parameters):
                query = urlencode(parameters)
                return request(base_url, "GET", f"/api/releases/export?{query}")

            # A bound half a millisecond after created_at must exclude it.
            after = (created + timedelta(microseconds=500)).isoformat()
            status, body = export(**{"from": after})
            self.assertEqual(status, 200)
            self.assertEqual(json.loads(body), [])
            # And half a millisecond before must include it.
            before = (created - timedelta(microseconds=500)).isoformat()
            status, body = export(**{"from": before})
            self.assertEqual(status, 200)
            self.assertEqual(
                [r["request_id"] for r in json.loads(body)], ["exp-precision"]
            )
            # to is exclusive at the exact instant.
            status, body = export(to=created.isoformat())
            self.assertEqual(status, 200)
            self.assertEqual(json.loads(body), [])

    def test_explicit_empty_request_id_returns_empty_result(self):
        with running_demo(0) as base_url:
            publish(base_url, "exp-empty-rid")
            status, body = request(
                base_url, "GET", "/api/releases/export?request_id="
            )
            self.assertEqual(status, 200)
            self.assertEqual(json.loads(body), [])
            status, body = request(
                base_url, "GET", "/api/releases/export?format=csv&request_id="
            )
            self.assertEqual(status, 200)
            self.assertEqual(
                body.strip().splitlines(),
                [
                    "release_id,request_id,dataset_id,filters,epsilon,"
                    "published_count,remaining_budget,created_at"
                ],
            )
            # Pure whitespace is still rejected.
            status, _ = request(
                base_url, "GET", "/api/releases/export?request_id=%20%20"
            )
            self.assertEqual(status, 422)


if __name__ == "__main__":
    unittest.main()
