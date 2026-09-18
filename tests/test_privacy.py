import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from contextlib import closing, contextmanager
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from privstat.database import initialize_database
from privstat.demo import running_demo
from privstat.privacy import (
    configured_budget,
    initialize_privacy_store,
    list_releases,
    publish_count,
)


def http_get(base_url, endpoint):
    with urlopen(base_url + endpoint, timeout=15) as response:
        return response.status, json.loads(response.read().decode("utf-8"))


def http_post(base_url, endpoint, payload, raw=None):
    data = raw if raw is not None else json.dumps(payload).encode("utf-8")
    request = Request(
        base_url + endpoint,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urlopen(request, timeout=15) as response:
            return response.status, json.loads(response.read().decode("utf-8"))
    except HTTPError as error:
        body = error.read().decode("utf-8")
        try:
            parsed = json.loads(body)
        except json.JSONDecodeError:
            parsed = body
        return error.code, parsed


@contextmanager
def demo_with_budget(total):
    old = os.environ.pop("PRIVSTAT_EPSILON_BUDGET", None)
    if total is not None:
        os.environ["PRIVSTAT_EPSILON_BUDGET"] = str(total)
    try:
        with running_demo(0) as base_url:
            yield base_url
    finally:
        if old is None:
            os.environ.pop("PRIVSTAT_EPSILON_BUDGET", None)
        else:
            os.environ["PRIVSTAT_EPSILON_BUDGET"] = old


class ScriptedRandom:
    """Minimal random.Random stand-in returning fixed [0, 1) draws."""

    def __init__(self, values):
        self._values = list(values)
        self._index = 0

    def random(self):
        value = self._values[self._index % len(self._values)]
        self._index += 1
        return value


class ReleaseEndpointTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._old_budget = os.environ.get("PRIVSTAT_EPSILON_BUDGET")
        os.environ["PRIVSTAT_EPSILON_BUDGET"] = "10"
        cls.service = running_demo(0)
        cls.base_url = cls.service.__enter__()

    @classmethod
    def tearDownClass(cls):
        cls.service.__exit__(None, None, None)
        if cls._old_budget is None:
            os.environ.pop("PRIVSTAT_EPSILON_BUDGET", None)
        else:
            os.environ["PRIVSTAT_EPSILON_BUDGET"] = cls._old_budget

    def test_publish_returns_sanitized_record(self):
        _, budget_before = http_get(self.base_url, "/api/privacy-budget")
        status, body = http_post(
            self.base_url,
            "/api/releases",
            {
                "request_id": "valid-001",
                "dataset_id": "retail-demo",
                "filters": [{"field": "region", "value": "north"}],
                "epsilon": 1.0,
            },
        )
        self.assertEqual(status, 200)
        self.assertEqual(
            set(body),
            {"release_id", "request", "published_count", "remaining_budget", "created_at"},
        )
        self.assertEqual(
            set(body["request"]), {"request_id", "dataset_id", "filters", "epsilon"}
        )
        self.assertIsInstance(body["published_count"], int)
        self.assertGreaterEqual(body["published_count"], 0)
        self.assertNotIn("S001", json.dumps(body))
        _, budget = http_get(self.base_url, "/api/privacy-budget")
        self.assertEqual(budget["initial"], 10.0)
        self.assertAlmostEqual(
            budget["used"] - budget_before["used"], 1.0, places=6
        )
        self.assertAlmostEqual(
            budget["remaining"], budget["initial"] - budget["used"], places=6
        )

    def test_empty_and_missing_filters_are_accepted(self):
        for filters in ([], None):
            status, body = http_post(
                self.base_url,
                "/api/releases",
                {
                    "request_id": f"empty-filters-{filters is None}",
                    "dataset_id": "retail-demo",
                    "filters": filters,
                    "epsilon": 0.1,
                },
            )
            self.assertEqual(status, 200, body)
            self.assertEqual(body["request"]["filters"], [])

    def test_validation_failures_return_422(self):
        cases = [
            {},
            {"request_id": "", "dataset_id": "retail-demo", "epsilon": 1.0},
            {"request_id": "   ", "dataset_id": "retail-demo", "epsilon": 1.0},
            {"request_id": "bad-dataset-type", "dataset_id": 1, "epsilon": 1.0},
            {"request_id": "eps-missing", "dataset_id": "retail-demo"},
            {"request_id": "eps-low", "dataset_id": "retail-demo", "epsilon": 0.05},
            {"request_id": "eps-high", "dataset_id": "retail-demo", "epsilon": 1.5},
            {"request_id": "eps-str", "dataset_id": "retail-demo", "epsilon": "1.0"},
            {"request_id": "eps-bool", "dataset_id": "retail-demo", "epsilon": True},
            {"request_id": "filters-map", "dataset_id": "retail-demo", "filters": {}},
            {
                "request_id": "member-filter",
                "dataset_id": "retail-demo",
                "filters": [{"field": "member_id", "value": "S001"}],
                "epsilon": 1.0,
            },
            {
                "request_id": "unknown-field",
                "dataset_id": "retail-demo",
                "filters": [{"field": "ssn", "value": "x"}],
                "epsilon": 1.0,
            },
            {
                "request_id": "non-string-value",
                "dataset_id": "retail-demo",
                "filters": [{"field": "region", "value": 3}],
                "epsilon": 1.0,
            },
            {
                "request_id": "empty-value",
                "dataset_id": "retail-demo",
                "filters": [{"field": "region", "value": "  "}],
                "epsilon": 1.0,
            },
            {
                "request_id": "dup-field",
                "dataset_id": "retail-demo",
                "filters": [
                    {"field": "region", "value": "north"},
                    {"field": "region", "value": "east"},
                ],
                "epsilon": 1.0,
            },
            {
                "request_id": "extra-key",
                "dataset_id": "retail-demo",
                "filters": [{"field": "region", "value": "north", "op": "eq"}],
                "epsilon": 1.0,
            },
        ]
        _, used_before = http_get(self.base_url, "/api/privacy-budget")
        for payload in cases:
            with self.subTest(request_id=payload.get("request_id")):
                status, body = http_post(self.base_url, "/api/releases", payload)
                self.assertEqual(status, 422, body)
                self.assertIn("error", body)
        status, body = http_post(
            self.base_url, "/api/releases", None, raw=b"{not json"
        )
        self.assertEqual(status, 422)
        status, body = http_post(
            self.base_url, "/api/releases", None, raw=b'["a", "b"]'
        )
        self.assertEqual(status, 422)
        _, used_after = http_get(self.base_url, "/api/privacy-budget")
        self.assertEqual(used_before["used"], used_after["used"])

    def test_unknown_dataset_is_404_and_costs_nothing(self):
        _, budget_before = http_get(self.base_url, "/api/privacy-budget")
        status, body = http_post(
            self.base_url,
            "/api/releases",
            {"request_id": "no-such-data", "dataset_id": "other", "epsilon": 1.0},
        )
        self.assertEqual(status, 404)
        self.assertIn("error", body)
        _, budget_after = http_get(self.base_url, "/api/privacy-budget")
        self.assertEqual(budget_before["used"], budget_after["used"])

    def test_reordered_identical_request_is_idempotent(self):
        payload_one = {
            "request_id": "idem-001",
            "dataset_id": "retail-demo",
            "filters": [
                {"field": "region", "value": "north"},
                {"field": "membership", "value": "standard"},
            ],
            "epsilon": 0.5,
        }
        payload_two = json.loads(json.dumps(payload_one))
        payload_two["filters"] = list(reversed(payload_one["filters"]))
        status, first = http_post(self.base_url, "/api/releases", payload_one)
        self.assertEqual(status, 200)
        _, used_after_first = http_get(self.base_url, "/api/privacy-budget")
        status, second = http_post(self.base_url, "/api/releases", payload_two)
        self.assertEqual(status, 200)
        self.assertEqual(second, first)
        _, used_after_second = http_get(self.base_url, "/api/privacy-budget")
        self.assertEqual(used_after_first["used"], used_after_second["used"])

    def test_same_id_with_different_request_is_409(self):
        payload = {
            "request_id": "conflict-id",
            "dataset_id": "retail-demo",
            "filters": [{"field": "region", "value": "north"}],
            "epsilon": 0.5,
        }
        status, _ = http_post(self.base_url, "/api/releases", payload)
        self.assertEqual(status, 200)
        mutated = json.loads(json.dumps(payload))
        mutated["epsilon"] = 0.3
        status, body = http_post(self.base_url, "/api/releases", mutated)
        self.assertEqual(status, 409)
        self.assertIn("error", body)
        mutated = json.loads(json.dumps(payload))
        mutated["filters"][0]["value"] = "south"
        status, _ = http_post(self.base_url, "/api/releases", mutated)
        self.assertEqual(status, 409)

    def test_history_is_newest_first(self):
        ids = ["history-a", "history-b", "history-c"]
        for request_id in ids:
            status, _ = http_post(
                self.base_url,
                "/api/releases",
                {
                    "request_id": request_id,
                    "dataset_id": "retail-demo",
                    "epsilon": 0.1,
                },
            )
            self.assertEqual(status, 200)
        _, releases = http_get(self.base_url, "/api/releases")
        returned_ids = [row["request"]["request_id"] for row in releases]
        for request_id in reversed(ids):
            self.assertIn(request_id, returned_ids)
        self.assertEqual(returned_ids[:3], list(reversed(ids)))

    def test_concurrent_identical_requests_charge_once(self):
        payload = {
            "request_id": "concurrent-same",
            "dataset_id": "retail-demo",
            "filters": [{"field": "region", "value": "west"}],
            "epsilon": 0.5,
        }
        _, budget_before = http_get(self.base_url, "/api/privacy-budget")
        results = []
        threads = []

        import threading

        def worker():
            results.append(http_post(self.base_url, "/api/releases", payload))

        for _ in range(8):
            thread = threading.Thread(target=worker)
            threads.append(thread)
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(len(results), 8)
        self.assertTrue(all(status == 200 for status, _ in results))
        release_ids = {body["release_id"] for _, body in results}
        self.assertEqual(len(release_ids), 1)
        published = {body["published_count"] for _, body in results}
        self.assertEqual(len(published), 1)
        _, budget_after = http_get(self.base_url, "/api/privacy-budget")
        self.assertAlmostEqual(
            budget_after["used"] - budget_before["used"], 0.5, places=6
        )
        _, releases = http_get(self.base_url, "/api/releases")
        self.assertEqual(
            sum(1 for r in releases if r["request"]["request_id"] == "concurrent-same"),
            1,
        )


class BudgetEnforcementTests(unittest.TestCase):
    def test_insufficient_budget_returns_409_and_leaves_no_record(self):
        with demo_with_budget(0.1) as base_url:
            status, first = http_post(
                base_url,
                "/api/releases",
                {"request_id": "spend-all", "dataset_id": "retail-demo", "epsilon": 0.1},
            )
            self.assertEqual(status, 200)
            self.assertAlmostEqual(first["remaining_budget"], 0.0, places=6)
            status, body = http_post(
                base_url,
                "/api/releases",
                {"request_id": "rejected-1", "dataset_id": "retail-demo", "epsilon": 0.1},
            )
            self.assertEqual(status, 409)
            self.assertIn("error", body)
            _, releases = http_get(base_url, "/api/releases")
            self.assertEqual([r["request"]["request_id"] for r in releases], ["spend-all"])
            _, budget = http_get(base_url, "/api/privacy-budget")
            self.assertAlmostEqual(budget["used"], 0.1, places=9)
            self.assertAlmostEqual(budget["remaining"], 0.0, places=9)
            # The rejected request id was never consumed.
            status, retry = http_post(
                base_url,
                "/api/releases",
                {"request_id": "rejected-1", "dataset_id": "retail-demo", "epsilon": 0.0},
            )
            self.assertEqual(status, 422)  # still fully validated, not a replay

    def test_concurrent_contenders_under_tight_budget(self):
        import threading

        with demo_with_budget(0.15) as base_url:
            results = []
            threads = []

            def worker(index):
                results.append(
                    http_post(
                        base_url,
                        "/api/releases",
                        {
                            "request_id": f"race-{index}",
                            "dataset_id": "retail-demo",
                            "epsilon": 0.1,
                        },
                    )
                )

            for index in range(4):
                thread = threading.Thread(target=worker, args=(index,))
                threads.append(thread)
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()
            statuses = sorted(status for status, _ in results)
            self.assertEqual(statuses, [200, 409, 409, 409])
            _, releases = http_get(base_url, "/api/releases")
            self.assertEqual(len(releases), 1)
            _, budget = http_get(base_url, "/api/privacy-budget")
            self.assertAlmostEqual(budget["used"], 0.1, places=9)


class PrivacyMechanismTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.path = Path(self.directory.name) / "privacy.sqlite3"
        initialize_database(self.path)
        initialize_privacy_store(self.path, 3.0)

    def tearDown(self):
        self.directory.cleanup()

    def payload(self, **overrides):
        payload = {
            "request_id": "unit-001",
            "dataset_id": "retail-demo",
            "filters": [],
            "epsilon": 0.5,
        }
        payload.update(overrides)
        return payload

    def test_noise_zero_recovers_true_count(self):
        result = publish_count(self.path, self.payload(), ScriptedRandom([0.0, 0.0]))
        # 24 synthetic members; first uniform draw at 0 forces zero noise.
        self.assertEqual(result["published_count"], 24)

    def test_rounding_is_half_up(self):
        # scale = 1/0.5 = 2; force noise of +0.6 -> true count 3 becomes 4.
        first_draw = 1 - 2.718281828459045 ** -0.3  # -scale*ln(1-u) = 0.6
        payload = self.payload(
            request_id="unit-round",
            filters=[
                {"field": "region", "value": "north"},
                {"field": "membership", "value": "standard"},
            ],
        )
        result = publish_count(self.path, payload, ScriptedRandom([first_draw, 0.0]))
        self.assertEqual(result["published_count"], 4)

    def test_negative_noise_is_floored_at_zero(self):
        # scale = 1/0.1 = 10; force noise of -10 on an empty group.
        first_draw = 1 - 2.718281828459045 ** -1.0  # magnitude 10
        payload = self.payload(
            request_id="unit-floor",
            filters=[{"field": "region", "value": "atlantis"}],
            epsilon=0.1,
        )
        result = publish_count(self.path, payload, ScriptedRandom([first_draw, 0.99]))
        self.assertEqual(result["published_count"], 0)

    def test_state_survives_restart_without_double_charge(self):
        result = publish_count(
            self.path,
            self.payload(
                request_id="restart-1",
                filters=[{"field": "region", "value": "east"}],
                epsilon=0.5,
            ),
        )
        initialize_database(self.path)
        initialize_privacy_store(self.path, 3.0)  # process "restart"
        replay = publish_count(
            self.path,
            self.payload(
                request_id="restart-1",
                filters=[{"field": "region", "value": "east"}],
                epsilon=0.5,
            ),
        )
        self.assertEqual(replay["release_id"], result["release_id"])
        self.assertEqual(replay["published_count"], result["published_count"])
        self.assertEqual(len(list_releases(self.path)), 1)

    def test_audit_table_cannot_hold_true_count_or_member_ids(self):
        publish_count(
            self.path,
            self.payload(
                request_id="audit-1",
                filters=[{"field": "region", "value": "north"}],
                epsilon=1.0,
            ),
            ScriptedRandom([0.0, 0.0]),
        )
        with closing(sqlite3.connect(self.path)) as connection:
            columns = {
                row[1]
                for row in connection.execute("PRAGMA table_info(releases)").fetchall()
            }
            self.assertEqual(
                columns,
                {
                    "release_id",
                    "request_id",
                    "request_key",
                    "request_json",
                    "published_count",
                    "remaining_budget",
                    "created_at",
                },
            )
            dump = "\n".join(
                str(row)
                for row in connection.execute("SELECT * FROM releases").fetchall()
            )
        self.assertNotIn("S001", dump)
        self.assertNotIn("member_id", dump)


class ConfigurationTests(unittest.TestCase):
    def test_default_budget(self):
        os.environ.pop("PRIVSTAT_EPSILON_BUDGET", None)
        self.assertEqual(configured_budget(), 3.0)

    def test_custom_budget(self):
        old = os.environ.pop("PRIVSTAT_EPSILON_BUDGET", None)
        try:
            os.environ["PRIVSTAT_EPSILON_BUDGET"] = "10.5"
            self.assertEqual(configured_budget(), 10.5)
        finally:
            if old is None:
                os.environ.pop("PRIVSTAT_EPSILON_BUDGET", None)
            else:
                os.environ["PRIVSTAT_EPSILON_BUDGET"] = old

    def test_invalid_budget_rejected(self):
        old = os.environ.pop("PRIVSTAT_EPSILON_BUDGET", None)
        try:
            for raw in ("0", "-1", "abc"):
                os.environ["PRIVSTAT_EPSILON_BUDGET"] = raw
                with self.assertRaises(RuntimeError):
                    configured_budget()
        finally:
            if old is None:
                os.environ.pop("PRIVSTAT_EPSILON_BUDGET", None)
            else:
                os.environ["PRIVSTAT_EPSILON_BUDGET"] = old


class DemoScriptTests(unittest.TestCase):
    def test_demo_publishes_once_and_prints_budget_history(self):
        repo_root = Path(__file__).resolve().parent.parent
        completed = subprocess.run(
            [sys.executable, "-m", "privstat.demo", "--port", "0"],
            cwd=repo_root,
            capture_output=True,
            text=True,
            timeout=60,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)
        output = completed.stdout
        self.assertIn("POST /api/releases", output)
        self.assertIn("published_count", output)
        self.assertIn("GET /api/privacy-budget", output)
        self.assertIn('"remaining": 2.0', output)
        self.assertIn("GET /api/releases", output)
        self.assertIn("demo-release-0001", output)
        self.assertIn("demo service closed", output)
        self.assertNotIn("S001", output)


if __name__ == "__main__":
    unittest.main()
