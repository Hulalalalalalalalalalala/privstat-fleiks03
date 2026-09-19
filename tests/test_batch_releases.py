import json
import sqlite3
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from datetime import datetime, timedelta
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from privstat.database import (
    DEFAULT_EPSILON_BUDGET,
    BatchConflictError,
    BudgetExceededError,
    ReleaseConflictError,
    budget_status,
    create_release,
    create_release_batch,
    initialize_database,
    list_releases,
)
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


def request(base_url, method, path, payload=None):
    data = json.dumps(payload).encode("utf-8") if payload is not None else None
    headers = {"Content-Type": "application/json"} if data is not None else {}
    req = Request(base_url + path, data=data, headers=headers, method=method)
    try:
        with urlopen(req, timeout=10) as response:
            return response.status, json.loads(response.read())
    except HTTPError as error:
        body = json.loads(error.read().decode("utf-8"))
        status = error.code
        error.close()
        return status, body


def get_json(base_url, path):
    with urlopen(base_url + path, timeout=10) as response:
        return json.loads(response.read())


def item(request_id, epsilon=0.3, filters=None):
    return {
        "request_id": request_id,
        "dataset_id": "retail-demo",
        "filters": filters,
        "epsilon": epsilon,
    }


def batch(base_url, batch_id, items_, **overrides):
    payload = {"batch_id": batch_id, "requests": items_}
    payload.update(overrides)
    return request(base_url, "POST", "/api/releases/batch", payload)


def valid_items(count, prefix="b", epsilon=0.3):
    return [item(f"{prefix}-{index}", epsilon) for index in range(count)]


class BatchEndpointTests(unittest.TestCase):
    def setUp(self):
        self.service = running_demo(0)
        self.base_url = self.service.__enter__()

    def tearDown(self):
        self.service.__exit__(None, None, None)

    def test_publish_returns_ordered_records_and_sequential_budget(self):
        before = get_json(self.base_url, "/api/privacy-budget")
        status, body = batch(
            self.base_url,
            "batch-order",
            [
                item("ord-1", 0.5, {"region": "north"}),
                item("ord-2", 0.3),
            ],
        )
        self.assertEqual(status, 201)
        self.assertEqual(
            set(body),
            {"batch_id", "releases", "total_epsilon", "remaining_budget", "created_at"},
        )
        self.assertEqual(body["batch_id"], "batch-order")
        self.assertEqual(body["total_epsilon"], 0.8)
        self.assertEqual([r["request_id"] for r in body["releases"]], ["ord-1", "ord-2"])
        first, second = body["releases"]
        # Budgets deduct in input order; the top-level value is the last item's.
        self.assertAlmostEqual(first["remaining_budget"], before["remaining_budget"] - 0.5)
        self.assertAlmostEqual(second["remaining_budget"], before["remaining_budget"] - 0.8)
        self.assertEqual(body["remaining_budget"], second["remaining_budget"])
        for release in body["releases"]:
            self.assertEqual(set(release), PUBLIC_FIELDS)
            self.assertNotIn("batch_id", release)
            self.assertIsInstance(release["published_count"], int)
            self.assertGreaterEqual(release["published_count"], 0)
        created_at = datetime.fromisoformat(body["created_at"])
        self.assertEqual(created_at.utcoffset(), timedelta(0))
        self.assertIn(".", body["created_at"].split("+")[0])
        self.assertEqual(first["created_at"], body["created_at"])
        self.assertEqual(second["created_at"], body["created_at"])
        after = get_json(self.base_url, "/api/privacy-budget")
        self.assertAlmostEqual(after["used_budget"] - before["used_budget"], 0.8)
        self.assertAlmostEqual(after["remaining_budget"], body["remaining_budget"])
        serialized = json.dumps(body)
        self.assertNotIn("S001", serialized)
        self.assertNotIn("true_count", serialized)

    def test_replay_returns_200_identical_without_resampling_or_deduction(self):
        requests_ = [item("rep-1", 0.4, {"membership": "plus"}), item("rep-2", 0.2)]
        first_status, first = batch(self.base_url, "batch-replay", requests_)
        self.assertEqual(first_status, 201)
        before = get_json(self.base_url, "/api/privacy-budget")
        # Same normalized request, different JSON key order in one filters dict.
        reordered = [
            item("rep-1", 0.4, dict(reversed(list({"membership": "plus"}.items())))),
            item("rep-2", 0.2),
        ]
        second_status, second = batch(self.base_url, "batch-replay", reordered)
        after = get_json(self.base_url, "/api/privacy-budget")
        self.assertEqual(second_status, 200)
        self.assertEqual(second, first)
        self.assertEqual(before, after)

    def test_reordered_items_are_a_different_request(self):
        requests_ = [item("ro-1", 0.2), item("ro-2", 0.2)]
        self.assertEqual(batch(self.base_url, "batch-reorder", requests_)[0], 201)
        status, _ = batch(self.base_url, "batch-reorder", list(reversed(requests_)))
        self.assertEqual(status, 409)

    def test_different_body_same_batch_id_returns_409(self):
        self.assertEqual(
            batch(self.base_url, "batch-conflict", valid_items(2, "cf"))[0], 201
        )
        status, _ = batch(self.base_url, "batch-conflict", valid_items(3, "cf"))
        self.assertEqual(status, 409)

    def test_invalid_payloads_return_422(self):
        cases = [
            {},
            {"batch_id": "batch-x"},
            {"requests": valid_items(1)},
            {"batch_id": "", "requests": valid_items(1)},
            {"batch_id": "   ", "requests": valid_items(1)},
            {"batch_id": 123, "requests": valid_items(1)},
            {"batch_id": None, "requests": valid_items(1)},
            {"batch_id": "batch-x", "requests": []},
            {"batch_id": "batch-x", "requests": valid_items(21)},
            {"batch_id": "batch-x", "requests": {}},
            {"batch_id": "batch-x", "requests": None},
            {"batch_id": "batch-x", "requests": [{"dataset_id": "retail-demo", "epsilon": 0.3}]},
            {"batch_id": "batch-x", "requests": [item("dup"), item("dup")]},
            {"batch_id": "batch-x", "requests": [dict(item("x"), epsilon=0.05)]},
            {"batch_id": "batch-x", "requests": [dict(item("x"), epsilon=1.5)]},
            {"batch_id": "batch-x", "requests": [dict(item("x"), epsilon="many")]},
            {"batch_id": "batch-x", "requests": [dict(item("x"), request_id="  ")]},
            {"batch_id": "batch-x", "requests": [dict(item("x"), request_id=7)]},
            {"batch_id": "batch-x", "requests": [dict(item("x"), filters={"member_id": "S001"})]},
            {"batch_id": "batch-x", "requests": [dict(item("x"), filters={"region": 5})]},
            {"batch_id": "batch-x", "requests": [dict(item("x"), bogus=1)]},
            {"batch_id": "batch-x", "requests": valid_items(1), "extra": 1},
        ]
        for payload in cases:
            with self.subTest(payload=payload):
                status, _ = request(
                    self.base_url, "POST", "/api/releases/batch", payload
                )
                self.assertEqual(status, 422)

    def test_unknown_dataset_fails_whole_batch_with_404(self):
        before = get_json(self.base_url, "/api/privacy-budget")
        payload = {
            "batch_id": "batch-unknown",
            "requests": [
                item("unk-ok", 0.2),
                {"request_id": "unk-bad", "dataset_id": "other", "filters": None, "epsilon": 0.2},
            ],
        }
        status, _ = request(self.base_url, "POST", "/api/releases/batch", payload)
        self.assertEqual(status, 404)
        # Neither item was written and no budget was spent.
        history = get_json(self.base_url, "/api/releases")
        self.assertNotIn("unk-ok", [r["request_id"] for r in history])
        self.assertEqual(get_json(self.base_url, "/api/privacy-budget"), before)

    def test_request_id_occupied_by_single_returns_409_without_partial(self):
        self.assertEqual(
            request(
                self.base_url,
                "POST",
                "/api/releases",
                {"request_id": "solo-occupied", "dataset_id": "retail-demo",
                 "filters": None, "epsilon": 0.2},
            )[0],
            201,
        )
        before = get_json(self.base_url, "/api/privacy-budget")
        status, _ = batch(
            self.base_url,
            "batch-occupies-single",
            [item("partial-new", 0.2), item("solo-occupied", 0.2)],
        )
        self.assertEqual(status, 409)
        history = get_json(self.base_url, "/api/releases")
        self.assertNotIn("partial-new", [r["request_id"] for r in history])
        self.assertEqual(get_json(self.base_url, "/api/privacy-budget"), before)
        # The failed batch left no placeholder: a different valid batch can
        # claim the same batch_id.
        status, _ = batch(
            self.base_url, "batch-occupies-single", [item("placeholder-free", 0.2)]
        )
        self.assertEqual(status, 201)

    def test_batch_request_ids_block_singles_and_later_batches(self):
        self.assertEqual(
            batch(self.base_url, "batch-shared-id", [item("shared-rid", 0.2,
                                                          {"region": "east"})])[0],
            201,
        )
        # An identical normalized single replays the batch-owned record
        # (200, no new deduction), matching the single endpoint's existing
        # idempotency; a different single conflicts with 409.
        identical = {
            "request_id": "shared-rid", "dataset_id": "retail-demo",
            "filters": {"region": "east"}, "epsilon": 0.2,
        }
        self.assertEqual(
            request(self.base_url, "POST", "/api/releases", identical)[0], 200
        )
        different = dict(identical, epsilon=0.3)
        self.assertEqual(
            request(self.base_url, "POST", "/api/releases", different)[0], 409
        )
        status, _ = batch(
            self.base_url, "batch-other-id", [item("shared-rid", 0.2)]
        )
        self.assertEqual(status, 409)

    def test_budget_shortfall_returns_409_without_state(self):
        base_url = self.base_url
        for index in range(3):
            self.assertEqual(
                request(
                    base_url,
                    "POST",
                    "/api/releases",
                    {"request_id": f"fill-{index}", "dataset_id": "retail-demo",
                     "filters": None, "epsilon": 1.0},
                )[0],
                201,
            )
        status, _ = batch(
            base_url,
            "batch-over-budget",
            [item("over-1", 0.1), item("over-2", 0.1)],
        )
        self.assertEqual(status, 409)
        budget = get_json(base_url, "/api/privacy-budget")
        self.assertEqual(budget["used_budget"], 3.0)
        history = get_json(base_url, "/api/releases")
        self.assertEqual(len(history), 3)
        self.assertEqual(
            {"over-1", "over-2"} & {r["request_id"] for r in history}, set()
        )

    def test_releases_immediately_visible_in_history_export_and_share(self):
        status, body = batch(
            self.base_url,
            "batch-visible",
            [item("vis-1", 0.2, {"region": "north"}), item("vis-2", 0.2)],
        )
        self.assertEqual(status, 201)
        history = get_json(self.base_url, "/api/releases")
        by_id = {r["request_id"]: r for r in history}
        self.assertIn("vis-1", by_id)
        self.assertIn("vis-2", by_id)
        # The stored remaining_budget is preserved verbatim in history.
        self.assertEqual(
            by_id["vis-1"]["remaining_budget"], body["releases"][0]["remaining_budget"]
        )
        exported = get_json(self.base_url, "/api/releases/export")
        exported_ids = {r["request_id"] for r in exported}
        self.assertIn("vis-1", exported_ids)
        self.assertIn("vis-2", exported_ids)
        status, share = request(
            self.base_url,
            "POST",
            "/api/shares",
            {"dataset_id": "retail-demo", "expires_at": "2027-01-01T00:00:00Z"},
        )
        self.assertEqual(status, 201)
        shared = get_json(
            self.base_url, f"/api/shares/{share['token']}/releases"
        )
        shared_ids = {r["request_id"] for r in shared}
        self.assertIn("vis-1", shared_ids)
        self.assertIn("vis-2", shared_ids)


class BatchConcurrencyTests(unittest.TestCase):
    def test_concurrent_distinct_batches_never_partially_succeed(self):
        with running_demo(0) as base_url:
            def attempt(index):
                return batch(
                    base_url,
                    f"batch-conc-{index}",
                    [item(f"conc-{index}-a", 0.5), item(f"conc-{index}-b", 0.5)],
                )[0]

            with ThreadPoolExecutor(max_workers=8) as pool:
                statuses = list(pool.map(attempt, range(6)))
            # Each committed batch spends 1.0; the 3.0 budget admits exactly 3.
            self.assertEqual(sorted(statuses), [201, 201, 201, 409, 409, 409])
            budget = get_json(base_url, "/api/privacy-budget")
            self.assertEqual(budget["used_budget"], 3.0)
            history = get_json(base_url, "/api/releases")
            # Committed batches always have both rows; no orphaned singles.
            for index in range(6):
                present = {
                    f"conc-{index}-a", f"conc-{index}-b"
                } & {r["request_id"] for r in history}
                self.assertIn(len(present), (0, 2))

    def test_concurrent_same_batch_id_commits_once(self):
        with running_demo(0) as base_url:
            payload_items = [item("dup-batch-1", 0.3), item("dup-batch-2", 0.3)]

            def attempt(_):
                return batch(base_url, "batch-conc-same", payload_items)

            with ThreadPoolExecutor(max_workers=6) as pool:
                results = list(pool.map(attempt, range(6)))
            statuses = [status for status, _ in results]
            self.assertEqual(statuses.count(201), 1)
            self.assertEqual(statuses.count(200), 5)
            bodies = [body for _, body in results]
            self.assertEqual({json.dumps(b, sort_keys=True) for b in bodies},
                             {json.dumps(bodies[0], sort_keys=True)})
            budget = get_json(base_url, "/api/privacy-budget")
            self.assertAlmostEqual(budget["used_budget"], 0.6)
            history = get_json(base_url, "/api/releases")
            self.assertEqual(
                sorted(r["request_id"] for r in history),
                ["dup-batch-1", "dup-batch-2"],
            )


class BatchStorageTests(unittest.TestCase):
    @staticmethod
    def _echo_count(true_count, epsilon):
        return true_count

    def test_atomicity_replay_conflict_and_restart_persistence(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.sqlite3"
            initialize_database(path)
            items = [
                {"request_id": "db-a", "dataset_id": "retail-demo",
                 "filters": {"region": "north"}, "epsilon": 0.6},
                {"request_id": "db-b", "dataset_id": "retail-demo",
                 "filters": {}, "epsilon": 0.4},
            ]
            response, created = create_release_batch(
                path,
                batch_id="db-batch",
                items=items,
                budget=DEFAULT_EPSILON_BUDGET,
                sample_count=self._echo_count,
            )
            self.assertTrue(created)
            self.assertEqual(response["total_epsilon"], 1.0)
            self.assertEqual(response["remaining_budget"], 2.0)
            self.assertEqual(
                [r["remaining_budget"] for r in response["releases"]], [2.4, 2.0]
            )
            # A replaying batch must not invoke the sampler at all.
            def fail_sampler(true_count, epsilon):
                raise AssertionError("replay must not re-sample")

            replay, replayed = create_release_batch(
                path,
                batch_id="db-batch",
                items=items,
                budget=DEFAULT_EPSILON_BUDGET,
                sample_count=fail_sampler,
            )
            self.assertFalse(replayed)
            self.assertEqual(replay, response)
            # A single release still sees the batch's request_ids.
            with self.assertRaises(ReleaseConflictError):
                create_release(
                    path,
                    request_id="db-a",
                    dataset_id="retail-demo",
                    filters={},
                    epsilon=0.1,
                    published_count=1,
                    budget=DEFAULT_EPSILON_BUDGET,
                )
            # Failed batches leave neither placeholder rows nor release rows.
            with self.assertRaises(BudgetExceededError):
                create_release_batch(
                    path,
                    batch_id="db-over",
                    items=[{"request_id": f"db-over-{i}", "dataset_id": "retail-demo",
                            "filters": {}, "epsilon": 1.0} for i in range(3)],
                    budget=DEFAULT_EPSILON_BUDGET,
                    sample_count=self._echo_count,
                )
            with closing(sqlite3.connect(path)) as connection:
                placeholder = connection.execute(
                    "SELECT COUNT(*) FROM release_batches WHERE batch_id = ?",
                    ("db-over",),
                ).fetchone()[0]
                orphan_rows = connection.execute(
                    "SELECT COUNT(*) FROM releases WHERE batch_id = ?", ("db-over",)
                ).fetchone()[0]
            self.assertEqual(placeholder, 0)
            self.assertEqual(orphan_rows, 0)
            with self.assertRaises(BatchConflictError):
                create_release_batch(
                    path,
                    batch_id="db-batch",
                    items=items + [
                        {"request_id": "db-c", "dataset_id": "retail-demo",
                         "filters": {}, "epsilon": 0.1}
                    ],
                    budget=DEFAULT_EPSILON_BUDGET,
                    sample_count=self._echo_count,
                )
            # Restart: state, idempotency and ordering all survive.
            initialize_database(path)
            self.assertEqual(budget_status(path, DEFAULT_EPSILON_BUDGET)["used_budget"], 1.0)
            after_restart, replayed = create_release_batch(
                path,
                batch_id="db-batch",
                items=items,
                budget=DEFAULT_EPSILON_BUDGET,
                sample_count=fail_sampler,
            )
            self.assertFalse(replayed)
            self.assertEqual(after_restart, response)
            history_ids = [r["request_id"] for r in list_releases(path)]
            self.assertEqual(history_ids[:2], ["db-b", "db-a"])

    def test_legacy_database_gains_batch_columns(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.sqlite3"
            initialize_database(path)
            # A single release predates batches and has no batch_id.
            record, created = create_release(
                path,
                request_id="legacy-single",
                dataset_id="retail-demo",
                filters={},
                epsilon=0.2,
                published_count=4,
                budget=DEFAULT_EPSILON_BUDGET,
            )
            self.assertTrue(created)
            initialize_database(path)
            response, created = create_release_batch(
                path,
                batch_id="post-migration",
                items=[{"request_id": "new-batch", "dataset_id": "retail-demo",
                        "filters": {}, "epsilon": 0.2}],
                budget=DEFAULT_EPSILON_BUDGET,
                sample_count=self._echo_count,
            )
            self.assertTrue(created)
            # Both remain visible; the legacy record's NULL batch_id never
            # collides with a real batch id.
            ids = {r["request_id"] for r in list_releases(path)}
            self.assertEqual(ids, {"legacy-single", "new-batch"})
            self.assertEqual(
                response["releases"][0]["remaining_budget"], 2.6
            )


if __name__ == "__main__":
    unittest.main()
