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
    BatchConflictError,
    budget_status,
    create_release,
    create_release_batch,
    initialize_database,
)
from privstat.demo import running_demo


BATCH_FIELDS = {
    "batch_id",
    "releases",
    "total_epsilon",
    "remaining_budget",
    "created_at",
}

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


def call(base_url, method, path, payload=None):
    data = json.dumps(payload).encode("utf-8") if payload is not None else None
    headers = {"Content-Type": "application/json"} if data is not None else {}
    request = Request(base_url + path, data=data, headers=headers, method=method)
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


def item(request_id, *, epsilon=0.5, filters=None, dataset_id="retail-demo"):
    return {
        "request_id": request_id,
        "dataset_id": dataset_id,
        "filters": filters,
        "epsilon": epsilon,
    }


def batch(base_url, batch_id, requests):
    return call(
        base_url,
        "POST",
        "/api/releases/batch",
        {"batch_id": batch_id, "requests": requests},
    )


class BatchEndpointTests(unittest.TestCase):
    def test_create_returns_ordered_releases_with_sequential_balances(self):
        with running_demo(0) as base_url:
            before = get_json(base_url, "/api/privacy-budget")
            requests = [
                item("b-create-1", epsilon=0.5, filters={"region": "north"}),
                item("b-create-2", epsilon=0.3),
            ]
            status, body = batch(base_url, "b-create", requests)
            self.assertEqual(status, 201)
            self.assertEqual(set(body), BATCH_FIELDS)
            self.assertEqual(body["batch_id"], "b-create")
            self.assertEqual(body["total_epsilon"], 0.8)
            self.assertEqual(len(body["releases"]), 2)
            self.assertEqual(
                [r["request_id"] for r in body["releases"]],
                ["b-create-1", "b-create-2"],
            )
            for release in body["releases"]:
                self.assertEqual(set(release), RELEASE_FIELDS)
                self.assertEqual(release["dataset_id"], "retail-demo")
                self.assertIsInstance(release["published_count"], int)
                self.assertGreaterEqual(release["published_count"], 0)
            # Each item keeps the balance after deducting it in order; the
            # top-level balance is exactly the last item's balance.
            self.assertEqual(
                [r["remaining_budget"] for r in body["releases"]], [2.5, 2.2]
            )
            self.assertEqual(body["remaining_budget"], 2.2)
            after = get_json(base_url, "/api/privacy-budget")
            self.assertAlmostEqual(after["used_budget"] - before["used_budget"], 0.8)
            self.assertAlmostEqual(
                body["remaining_budget"], after["remaining_budget"]
            )
            created_at = body["created_at"]
            self.assertIn(".", created_at)
            self.assertTrue(created_at.endswith("+00:00"))
            # All releases share one canonical creation instant.
            self.assertEqual(
                {r["created_at"] for r in body["releases"]}, {created_at}
            )
            # No true counts or row-level data leave the service.
            serialized = json.dumps(body)
            self.assertNotIn("S001", serialized)
            self.assertNotIn("true_count", serialized)

    def test_releases_are_immediately_visible_in_history_and_export(self):
        with running_demo(0) as base_url:
            status, body = batch(
                base_url,
                "b-visible",
                [
                    item("b-visible-1", epsilon=0.2, filters={"membership": "plus"}),
                    item("b-visible-2", epsilon=0.2, filters={"region": "south"}),
                ],
            )
            self.assertEqual(status, 201)
            history = get_json(base_url, "/api/releases")
            by_id = {r["request_id"]: r for r in history}
            for release in body["releases"]:
                stored = by_id[release["request_id"]]
                self.assertEqual(stored, release)
            status, exported = call(
                base_url,
                "GET",
                "/api/releases/export?request_id=b-visible-2",
            )
            self.assertEqual(status, 200)
            self.assertEqual(
                [r["request_id"] for r in exported], ["b-visible-2"]
            )
            self.assertEqual(
                exported[0]["remaining_budget"],
                body["releases"][1]["remaining_budget"],
            )
            self.assertNotIn("S001", json.dumps(exported))

    def test_replay_returns_200_with_first_response_without_resampling(self):
        with running_demo(0) as base_url:
            requests = [
                item("b-replay-1", epsilon=0.2, filters={"region": "north"}),
                item("b-replay-2", epsilon=0.4),
            ]
            first_status, first = batch(base_url, "b-replay", requests)
            self.assertEqual(first_status, 201)
            budget_before = get_json(base_url, "/api/privacy-budget")
            # Same normalized batch, reordered JSON text and filters text.
            second_status, second = batch(
                base_url,
                "b-replay",
                [
                    dict(requests[0]),
                    dict(requests[1], filters=dict(requests[1]["filters"] or {})),
                ],
            )
            budget_after = get_json(base_url, "/api/privacy-budget")
            self.assertEqual(second_status, 200)
            self.assertEqual(second, first)
            self.assertEqual(budget_before, budget_after)
            self.assertEqual(
                [r["release_id"] for r in second["releases"]],
                [r["release_id"] for r in first["releases"]],
            )
            # A third retry still replays the exact first response.
            third_status, third = batch(base_url, "b-replay", requests)
            self.assertEqual(third_status, 200)
            self.assertEqual(third, first)

    def test_same_batch_id_with_different_requests_returns_409(self):
        with running_demo(0) as base_url:
            requests = [item("b-diff-1", epsilon=0.2), item("b-diff-2", epsilon=0.2)]
            self.assertEqual(batch(base_url, "b-diff", requests)[0], 201)
            changed = [requests[0], dict(requests[1], epsilon=0.3)]
            status, _ = batch(base_url, "b-diff", changed)
            self.assertEqual(status, 409)
            # Different request set (order swapped content / another id).
            other = [requests[1], item("b-diff-3", epsilon=0.2)]
            status, _ = batch(base_url, "b-diff", other)
            self.assertEqual(status, 409)
            # The conflict wrote nothing extra and replaying still works.
            replay_status, replay = batch(base_url, "b-diff", requests)
            self.assertEqual(replay_status, 200)
            self.assertEqual(
                [r["request_id"] for r in replay["releases"]],
                ["b-diff-1", "b-diff-2"],
            )

    def test_request_id_shared_with_single_release_conflicts_both_ways(self):
        with running_demo(0) as base_url:
            single = item("shared-id", epsilon=0.2, filters={"region": "north"})
            status, single_body = call(base_url, "POST", "/api/releases", single)
            self.assertEqual(status, 201)
            # A batch cannot reuse the single release's identity.
            status, _ = batch(
                base_url, "b-from-single", [item("other-id", epsilon=0.2), single]
            )
            self.assertEqual(status, 409)
            self.assertEqual(
                [r["request_id"] for r in get_json(base_url, "/api/releases")],
                ["shared-id"],
            )
            # The batch first: then single conflicts on mismatch and replays
            # on an identical normalized request.
            status, batch_body = batch(
                base_url, "b-to-single", [item("batch-side-id", epsilon=0.3)]
            )
            self.assertEqual(status, 201)
            status, _ = call(
                base_url,
                "POST",
                "/api/releases",
                item("batch-side-id", epsilon=0.4),
            )
            self.assertEqual(status, 409)
            status, replay = call(
                base_url,
                "POST",
                "/api/releases",
                item("batch-side-id", epsilon=0.3),
            )
            self.assertEqual(status, 200)
            self.assertEqual(
                replay["release_id"], batch_body["releases"][0]["release_id"]
            )

    def test_request_id_used_by_another_batch_returns_409(self):
        with running_demo(0) as base_url:
            self.assertEqual(
                batch(base_url, "b-a", [item("cross-1", epsilon=0.2)])[0], 201
            )
            status, _ = batch(
                base_url,
                "b-b",
                [item("cross-2", epsilon=0.2), item("cross-1", epsilon=0.2)],
            )
            self.assertEqual(status, 409)
            history = get_json(base_url, "/api/releases")
            self.assertEqual([r["request_id"] for r in history], ["cross-1"])

    def test_insufficient_budget_rejects_whole_batch_without_rows(self):
        with mock.patch.dict(os.environ, {"PRIVSTAT_EPSILON_BUDGET": "1.0"}):
            with running_demo(0) as base_url:
                status, _ = batch(
                    base_url,
                    "b-overflow",
                    [item("b-over-1", epsilon=0.5), item("b-over-2", epsilon=0.6)],
                )
                self.assertEqual(status, 409)
                budget = get_json(base_url, "/api/privacy-budget")
                self.assertEqual(budget["used_budget"], 0.0)
                self.assertEqual(budget["remaining_budget"], 1.0)
                self.assertEqual(get_json(base_url, "/api/releases"), [])
                # The rejected batch_id leaves no placeholder: a different
                # request list under it is not a replay conflict.
                status, body = batch(
                    base_url,
                    "b-overflow",
                    [item("b-over-1", epsilon=0.5)],
                )
                self.assertEqual(status, 201)
                self.assertEqual(body["remaining_budget"], 0.5)

    def test_unknown_dataset_returns_404_for_whole_batch(self):
        with running_demo(0) as base_url:
            requests = [
                item("b-404-1", epsilon=0.2),
                item("b-404-2", epsilon=0.2, dataset_id="other"),
            ]
            status, _ = batch(base_url, "b-404", requests)
            self.assertEqual(status, 404)
            self.assertEqual(get_json(base_url, "/api/releases"), [])

    def test_invalid_payloads_return_422(self):
        valid_items = [item("b-422-a"), item("b-422-b")]
        cases = [
            {},
            {"requests": valid_items},
            {"batch_id": "", "requests": valid_items},
            {"batch_id": "   ", "requests": valid_items},
            {"batch_id": 123, "requests": valid_items},
            {"batch_id": None, "requests": valid_items},
            {"batch_id": "b"},
            {"batch_id": "b", "requests": []},
            {"batch_id": "b", "requests": "not-a-list"},
            {
                "batch_id": "b",
                "requests": [item(f"b-many-{i}") for i in range(21)],
            },
            {"batch_id": "b", "requests": [{"**": 1}]},
            {
                "batch_id": "b",
                "requests": [
                    item("b-sub-ok", epsilon=0.2),
                    {"request_id": "", "dataset_id": "retail-demo", "epsilon": 0.2},
                ],
            },
            {
                "batch_id": "b",
                "requests": [
                    {
                        "request_id": "b-sub-bad-eps",
                        "dataset_id": "retail-demo",
                        "epsilon": 1.5,
                    }
                ],
            },
            {
                "batch_id": "b",
                "requests": [
                    {
                        "request_id": "b-sub-bad-filter",
                        "dataset_id": "retail-demo",
                        "epsilon": 0.2,
                        "filters": {"member_id": "S001"},
                    }
                ],
            },
            {
                "batch_id": "b",
                "requests": [
                    {
                        "request_id": "b-sub-missing",
                        "epsilon": 0.2,
                    }
                ],
            },
            {
                "batch_id": "b",
                "requests": [
                    dict(item("b-extra-sub", epsilon=0.2), unexpected=True)
                ],
            },
            {
                "batch_id": "b",
                "requests": valid_items,
                "unexpected": True,
            },
            {
                "batch_id": "b",
                "requests": [
                    item("b-dup", epsilon=0.2),
                    item("b-dup", epsilon=0.3),
                ],
            },
        ]
        with running_demo(0) as base_url:
            for payload in cases:
                with self.subTest(payload=payload):
                    status, _ = call(
                        base_url, "POST", "/api/releases/batch", payload
                    )
                    self.assertEqual(status, 422)
            self.assertEqual(get_json(base_url, "/api/releases"), [])


class BatchConcurrencyTests(unittest.TestCase):
    def test_same_batch_id_concurrent_commits_once(self):
        with running_demo(0) as base_url:
            payload = {
                "batch_id": "conc-batch",
                "requests": [
                    item("conc-batch-1", epsilon=0.5),
                    item("conc-batch-2", epsilon=0.5),
                ],
            }

            def attempt(_):
                return call(
                    base_url, "POST", "/api/releases/batch", payload
                )

            with ThreadPoolExecutor(max_workers=6) as pool:
                results = list(pool.map(attempt, range(6)))
            statuses = sorted(status for status, _ in results)
            self.assertEqual(statuses, [200, 200, 200, 200, 200, 201])
            bodies = [body for _, body in results]
            self.assertEqual(
                len({json.dumps(b, sort_keys=True) for b in bodies}), 1
            )
            budget = get_json(base_url, "/api/privacy-budget")
            self.assertEqual(budget["used_budget"], 1.0)
            self.assertEqual(len(get_json(base_url, "/api/releases")), 2)

    def test_distinct_batches_never_overspend_budget(self):
        with running_demo(0) as base_url:
            def attempt(index):
                return batch(
                    base_url,
                    f"conc-budget-{index}",
                    [
                        item(f"conc-budget-{index}-a", epsilon=1.0),
                        item(f"conc-budget-{index}-b", epsilon=1.0),
                    ],
                )

            with ThreadPoolExecutor(max_workers=6) as pool:
                results = list(pool.map(attempt, range(3)))
            self.assertEqual(sorted(status for status, _ in results), [201, 409, 409])
            winner = next(body for status, body in results if status == 201)
            # Sequential balances inside the winning batch are preserved.
            self.assertEqual(
                [r["remaining_budget"] for r in winner["releases"]], [2.0, 1.0]
            )
            self.assertEqual(winner["remaining_budget"], 1.0)
            budget = get_json(base_url, "/api/privacy-budget")
            self.assertEqual(budget["used_budget"], 2.0)
            self.assertEqual(budget["remaining_budget"], 1.0)
            self.assertEqual(len(get_json(base_url, "/api/releases")), 2)

    def test_batches_sharing_request_id_commit_exactly_one(self):
        with running_demo(0) as base_url:
            def attempt(index):
                return batch(
                    base_url,
                    f"conc-cross-{index}",
                    [
                        item("conc-shared-rid", epsilon=0.2),
                        item(f"conc-cross-own-{index}", epsilon=0.2),
                    ],
                )

            with ThreadPoolExecutor(max_workers=4) as pool:
                results = list(pool.map(attempt, range(4)))
            self.assertEqual(sorted(status for status, _ in results).count(201), 1)
            self.assertEqual(sorted(status for status, _ in results).count(409), 3)
            history = get_json(base_url, "/api/releases")
            self.assertEqual(len(history), 2)
            self.assertIn("conc-shared-rid", [r["request_id"] for r in history])


class BatchStorageRestartTests(unittest.TestCase):
    def _items(self, *request_ids):
        return [
            {
                "request_id": rid,
                "dataset_id": "retail-demo",
                "filters": {"region": "north"},
                "epsilon": 0.4,
                "true_count": 7,
            }
            for rid in request_ids
        ]

    def test_batch_replay_and_conflicts_survive_reopen(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.sqlite3"
            initialize_database(path)
            header, records, created = create_release_batch(
                path,
                batch_id="persist-batch",
                items=self._items("persist-1", "persist-2"),
                budget=DEFAULT_EPSILON_BUDGET,
                sample_count=lambda true_count, epsilon: 42,
            )
            self.assertTrue(created)
            self.assertEqual([r["published_count"] for r in records], [42, 42])
            self.assertEqual(
                [r["remaining_budget"] for r in records], [2.6, 2.2]
            )
            self.assertEqual(header["total_epsilon"], 0.8)
            self.assertEqual(header["remaining_budget"], 2.2)
            # Restart: fresh connections against the same file.
            initialize_database(path)
            replay_header, replay_records, created = create_release_batch(
                path,
                batch_id="persist-batch",
                items=self._items("persist-1", "persist-2"),
                budget=DEFAULT_EPSILON_BUDGET,
                sample_count=lambda true_count, epsilon: self.fail(
                    "replay must not resample"
                ),
            )
            self.assertFalse(created)
            self.assertEqual(replay_header["created_at"], header["created_at"])
            self.assertEqual(
                [r["release_id"] for r in replay_records],
                [r["release_id"] for r in records],
            )
            self.assertEqual(
                [r["published_count"] for r in replay_records], [42, 42]
            )
            self.assertEqual(budget_status(path, DEFAULT_EPSILON_BUDGET)["used_budget"], 0.8)
            # A different request list under the same batch_id conflicts.
            with self.assertRaises(BatchConflictError) as ctx:
                create_release_batch(
                    path,
                    batch_id="persist-batch",
                    items=self._items("persist-1", "persist-3"),
                    budget=DEFAULT_EPSILON_BUDGET,
                    sample_count=lambda true_count, epsilon: 99,
                )
            self.assertEqual(ctx.exception.reason, "replay_mismatch")
            # A new batch cannot reuse a persisted request_id.
            with self.assertRaises(BatchConflictError) as ctx:
                create_release_batch(
                    path,
                    batch_id="other-batch",
                    items=self._items("persist-1"),
                    budget=DEFAULT_EPSILON_BUDGET,
                    sample_count=lambda true_count, epsilon: 99,
                )
            self.assertEqual(ctx.exception.reason, "request_id_occupied")
            self.assertEqual(budget_status(path, DEFAULT_EPSILON_BUDGET)["used_budget"], 0.8)

    def test_failed_batch_leaves_no_placeholder_after_restart(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.sqlite3"
            initialize_database(path)
            # Occupy the identity with a single release, then fail a batch.
            create_release(
                path,
                request_id="single-first",
                dataset_id="retail-demo",
                filters={},
                epsilon=0.2,
                published_count=3,
                budget=DEFAULT_EPSILON_BUDGET,
            )
            with self.assertRaises(BatchConflictError):
                create_release_batch(
                    path,
                    batch_id="doomed-batch",
                    items=self._items("single-first"),
                    budget=DEFAULT_EPSILON_BUDGET,
                    sample_count=lambda true_count, epsilon: 99,
                )
            initialize_database(path)
            # The failed batch_id is free for a fresh, different batch.
            header, records, created = create_release_batch(
                path,
                batch_id="doomed-batch",
                items=self._items("fresh-after-fail"),
                budget=DEFAULT_EPSILON_BUDGET,
                sample_count=lambda true_count, epsilon: 5,
            )
            self.assertTrue(created)
            self.assertEqual(records[0]["published_count"], 5)


if __name__ == "__main__":
    unittest.main()
