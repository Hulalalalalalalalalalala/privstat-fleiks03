import json
import os
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest import mock
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from privstat.database import (
    DEFAULT_EPSILON_BUDGET,
    ReleaseConflictError,
    budget_status,
    create_release,
    initialize_database,
)
from privstat.demo import running_demo


def post_release(base_url, payload):
    request = Request(
        base_url + "/api/releases",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urlopen(request, timeout=10) as response:
            return response.status, json.loads(response.read())
    except HTTPError as error:
        body = json.loads(error.read().decode("utf-8"))
        status = error.code
        error.close()
        return status, body


def get_json(base_url, path):
    with urlopen(base_url + path, timeout=10) as response:
        return json.loads(response.read())


VALID = {
    "request_id": "req-shared",
    "dataset_id": "retail-demo",
    "filters": {"region": "north"},
    "epsilon": 0.5,
}


class ReleaseEndpointTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.service = running_demo(0)
        cls.base_url = cls.service.__enter__()

    @classmethod
    def tearDownClass(cls):
        cls.service.__exit__(None, None, None)

    def test_publish_returns_record_and_consumes_budget(self):
        before = get_json(self.base_url, "/api/privacy-budget")
        status, body = post_release(self.base_url, dict(VALID, request_id="t-publish"))
        self.assertEqual(status, 201)
        self.assertEqual(body["request_id"], "t-publish")
        self.assertEqual(body["dataset_id"], "retail-demo")
        self.assertEqual(body["filters"], {"region": "north"})
        self.assertEqual(body["epsilon"], 0.5)
        self.assertIsInstance(body["published_count"], int)
        self.assertGreaterEqual(body["published_count"], 0)
        self.assertTrue(body["release_id"])
        self.assertTrue(body["created_at"])
        after = get_json(self.base_url, "/api/privacy-budget")
        self.assertAlmostEqual(after["used_budget"] - before["used_budget"], 0.5)
        self.assertAlmostEqual(body["remaining_budget"], after["remaining_budget"])
        self.assertAlmostEqual(
            after["initial_budget"] - after["used_budget"], after["remaining_budget"]
        )
        serialized = json.dumps(body)
        self.assertNotIn("S001", serialized)
        self.assertNotIn("true_count", serialized)

    def test_idempotent_replay_returns_original_without_new_deduction(self):
        payload = dict(VALID, request_id="t-replay", epsilon=0.2)
        first_status, first = post_release(self.base_url, payload)
        before = get_json(self.base_url, "/api/privacy-budget")
        # Same normalized request, different JSON key order in filters.
        reordered = dict(payload, filters={"region": "north"})
        second_status, second = post_release(self.base_url, reordered)
        after = get_json(self.base_url, "/api/privacy-budget")
        self.assertEqual(first_status, 201)
        self.assertEqual(second_status, 200)
        self.assertEqual(first["release_id"], second["release_id"])
        self.assertEqual(first["published_count"], second["published_count"])
        self.assertEqual(before, after)

    def test_conflicting_request_id_returns_409(self):
        payload = dict(VALID, request_id="t-conflict", epsilon=0.2)
        self.assertEqual(post_release(self.base_url, payload)[0], 201)
        status, _ = post_release(self.base_url, dict(payload, epsilon=0.3))
        self.assertEqual(status, 409)
        status, _ = post_release(
            self.base_url, dict(payload, filters={"region": "south"})
        )
        self.assertEqual(status, 409)

    def test_invalid_requests_return_422(self):
        cases = [
            dict(VALID, request_id=""),
            dict(VALID, request_id="   "),
            dict(VALID, request_id=123),
            dict(VALID, dataset_id=""),
            dict(VALID, epsilon=0.05),
            dict(VALID, epsilon=1.5),
            dict(VALID, epsilon="many"),
            dict(VALID, filters={"member_id": "S001"}),
            dict(VALID, filters={"region": 5}),
            dict(VALID, filters={"unknown_field": "x"}),
            dict(VALID, filters=["region"]),
            {key: value for key, value in VALID.items() if key != "request_id"},
            {key: value for key, value in VALID.items() if key != "epsilon"},
        ]
        for payload in cases:
            with self.subTest(payload=payload):
                status, _ = post_release(self.base_url, payload)
                self.assertEqual(status, 422)

    def test_unknown_dataset_returns_404(self):
        status, _ = post_release(
            self.base_url, dict(VALID, request_id="t-unknown", dataset_id="other")
        )
        self.assertEqual(status, 404)

    def test_history_lists_releases_newest_first(self):
        releases = get_json(self.base_url, "/api/releases")
        self.assertGreaterEqual(len(releases), 1)
        timestamps = [release["created_at"] for release in releases]
        self.assertEqual(timestamps, sorted(timestamps, reverse=True))
        for release in releases:
            self.assertEqual(
                set(release),
                {
                    "release_id",
                    "request_id",
                    "dataset_id",
                    "filters",
                    "epsilon",
                    "published_count",
                    "remaining_budget",
                    "created_at",
                },
            )
        self.assertNotIn("S001", json.dumps(releases))


class BudgetAndConcurrencyTests(unittest.TestCase):
    def test_budget_exhaustion_returns_409_without_record(self):
        with running_demo(0) as base_url:
            for index in range(3):
                status, _ = post_release(
                    base_url,
                    {
                        "request_id": f"exhaust-{index}",
                        "dataset_id": "retail-demo",
                        "filters": None,
                        "epsilon": 1.0,
                    },
                )
                self.assertEqual(status, 201)
            status, _ = post_release(
                base_url,
                {
                    "request_id": "exhaust-overflow",
                    "dataset_id": "retail-demo",
                    "filters": None,
                    "epsilon": 0.1,
                },
            )
            self.assertEqual(status, 409)
            budget = get_json(base_url, "/api/privacy-budget")
            self.assertEqual(budget["used_budget"], 3.0)
            self.assertEqual(budget["remaining_budget"], 0.0)
            self.assertEqual(len(get_json(base_url, "/api/releases")), 3)

    def test_concurrent_distinct_requests_stay_within_budget(self):
        with running_demo(0) as base_url:
            def attempt(index):
                return post_release(
                    base_url,
                    {
                        "request_id": f"concurrent-{index}",
                        "dataset_id": "retail-demo",
                        "filters": None,
                        "epsilon": 1.0,
                    },
                )[0]

            with ThreadPoolExecutor(max_workers=6) as pool:
                statuses = list(pool.map(attempt, range(6)))
            self.assertEqual(sorted(statuses), [201, 201, 201, 409, 409, 409])
            budget = get_json(base_url, "/api/privacy-budget")
            self.assertEqual(budget["used_budget"], 3.0)

    def test_concurrent_duplicate_request_id_deducts_once(self):
        with running_demo(0) as base_url:
            payload = {
                "request_id": "concurrent-dup",
                "dataset_id": "retail-demo",
                "filters": {"membership": "plus"},
                "epsilon": 0.5,
            }
            with ThreadPoolExecutor(max_workers=4) as pool:
                results = list(pool.map(lambda _: post_release(base_url, payload), range(4)))
            self.assertEqual(sorted(status for status, _ in results), [200, 200, 200, 201])
            self.assertEqual(len({body["release_id"] for _, body in results}), 1)
            budget = get_json(base_url, "/api/privacy-budget")
            self.assertEqual(budget["used_budget"], 0.5)

    def test_budget_is_configurable_via_environment(self):
        with mock.patch.dict(os.environ, {"PRIVSTAT_EPSILON_BUDGET": "1.0"}):
            with running_demo(0) as base_url:
                budget = get_json(base_url, "/api/privacy-budget")
                self.assertEqual(budget["initial_budget"], 1.0)
                payload = {
                    "request_id": "env-1",
                    "dataset_id": "retail-demo",
                    "filters": None,
                    "epsilon": 1.0,
                }
                self.assertEqual(post_release(base_url, payload)[0], 201)
                status, _ = post_release(
                    base_url, dict(payload, request_id="env-2", epsilon=0.1)
                )
                self.assertEqual(status, 409)


class RestartPersistenceTests(unittest.TestCase):
    def test_releases_and_budget_survive_reopen(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.sqlite3"
            initialize_database(path)
            record, created = create_release(
                path,
                request_id="persist",
                dataset_id="retail-demo",
                filters={"region": "east"},
                epsilon=0.7,
                published_count=9,
                budget=DEFAULT_EPSILON_BUDGET,
            )
            self.assertTrue(created)
            # Simulate a restart: fresh connections against the same file.
            status = budget_status(path, DEFAULT_EPSILON_BUDGET)
            self.assertEqual(status["used_budget"], 0.7)
            self.assertEqual(status["remaining_budget"], 2.3)
            replay, created = create_release(
                path,
                request_id="persist",
                dataset_id="retail-demo",
                filters={"region": "east"},
                epsilon=0.7,
                published_count=3,
                budget=DEFAULT_EPSILON_BUDGET,
            )
            self.assertFalse(created)
            self.assertEqual(replay["release_id"], record["release_id"])
            self.assertEqual(replay["published_count"], 9)
            self.assertEqual(budget_status(path, DEFAULT_EPSILON_BUDGET)["used_budget"], 0.7)
            with self.assertRaises(ReleaseConflictError):
                create_release(
                    path,
                    request_id="persist",
                    dataset_id="retail-demo",
                    filters={"region": "east"},
                    epsilon=0.8,
                    published_count=3,
                    budget=DEFAULT_EPSILON_BUDGET,
                )


if __name__ == "__main__":
    unittest.main()
