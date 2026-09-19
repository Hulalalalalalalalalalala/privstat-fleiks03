import csv
import io
import json
import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from privstat.database import DATASET_ID, export_releases, initialize_database
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
CSV_HEADER = [
    "release_id",
    "request_id",
    "dataset_id",
    "filters",
    "epsilon",
    "published_count",
    "remaining_budget",
    "created_at",
]


def get_export(base_url, **params):
    url = base_url + "/api/releases/export"
    query = urlencode(params)
    if query:
        url += "?" + query
    try:
        with urlopen(url, timeout=10) as response:
            return response.status, response.headers.get("Content-Type"), response.read().decode("utf-8")
    except HTTPError as error:
        body = error.read().decode("utf-8")
        status = error.code
        error.close()
        return status, error.headers.get("Content-Type"), body


def seed_release(base_url, request_id, *, epsilon=0.2, filters=None):
    status, body = post_release(
        base_url,
        {
            "request_id": request_id,
            "dataset_id": DATASET_ID,
            "filters": filters,
            "epsilon": epsilon,
        },
    )
    assert status in (200, 201), body
    return body


class ExportEndpointTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.service = running_demo(0)
        cls.base_url = cls.service.__enter__()

    @classmethod
    def tearDownClass(cls):
        cls.service.__exit__(None, None, None)

    def test_json_default_returns_public_records_newest_first(self):
        first = seed_release(self.base_url, "exp-json-1", filters={"region": "north"})
        second = seed_release(self.base_url, "exp-json-2", filters=None)
        status, content_type, body = get_export(
            self.base_url, request_id="exp-json-1", format="json"
        )
        self.assertEqual(status, 200)
        self.assertIn("application/json", content_type)
        records = json.loads(body)
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["release_id"], first["release_id"])
        self.assertEqual(set(records[0]), PUBLIC_FIELDS)

        status, _, body = get_export(self.base_url, limit=100)
        records = json.loads(body)
        self.assertGreaterEqual(len(records), 2)
        self.assertEqual(records[0]["release_id"], second["release_id"])
        self.assertEqual(records[1]["release_id"], first["release_id"])
        timestamps = [record["created_at"] for record in records]
        self.assertEqual(timestamps, sorted(timestamps, reverse=True))
        serialized = json.dumps(records)
        self.assertNotIn("member_id", serialized)
        self.assertNotIn("true_count", serialized)
        self.assertNotIn("S001", serialized)

    def test_format_defaults_to_json(self):
        seed_release(self.base_url, "exp-default-format")
        status, content_type, body = get_export(
            self.base_url, request_id="exp-default-format"
        )
        self.assertEqual(status, 200)
        self.assertIn("application/json", content_type)
        self.assertEqual(json.loads(body)[0]["request_id"], "exp-default-format")

    def test_csv_has_fixed_header_and_normalized_filters(self):
        seed_release(
            self.base_url,
            "exp-csv-1",
            filters={"region": "north", "membership": "plus"},
        )
        seed_release(self.base_url, "exp-csv-2", filters={"region": "a,b"})
        status, content_type, body = get_export(
            self.base_url,
            request_id="exp-csv-1",
            format="csv",
        )
        self.assertEqual(status, 200)
        self.assertIn("text/csv", content_type)
        self.assertIn("utf-8", content_type.lower())
        reader = csv.reader(io.StringIO(body))
        rows = list(reader)
        self.assertEqual(rows[0], CSV_HEADER)
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[1][1], "exp-csv-1")
        self.assertEqual(rows[1][2], DATASET_ID)
        # Filters use canonical normalized JSON: sorted keys, compact separators.
        self.assertEqual(rows[1][3], '{"membership":"plus","region":"north"}')
        self.assertEqual(json.loads(rows[1][3]),
                         {"region": "north", "membership": "plus"})

        status, _, body = get_export(
            self.base_url, request_id="exp-csv-2", format="csv"
        )
        rows = list(csv.reader(io.StringIO(body)))
        self.assertEqual(rows[0], CSV_HEADER)
        self.assertEqual(json.loads(rows[1][3]), {"region": "a,b"})

    def test_request_id_without_match_is_empty_200(self):
        status, _, body = get_export(
            self.base_url, request_id="exp-no-such-request", format="json"
        )
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(body), [])

        status, _, body = get_export(
            self.base_url, request_id="exp-no-such-request", format="csv"
        )
        self.assertEqual(status, 200)
        rows = list(csv.reader(io.StringIO(body)))
        self.assertEqual(rows, [CSV_HEADER])

    def test_dataset_id_only_allows_retail_demo(self):
        seed_release(self.base_url, "exp-dataset-ok")
        status, _, body = get_export(
            self.base_url, dataset_id=DATASET_ID, request_id="exp-dataset-ok"
        )
        self.assertEqual(status, 200)
        self.assertEqual(len(json.loads(body)), 1)
        for invalid in ("other", "", "RETAIL-DEMO"):
            with self.subTest(dataset_id=invalid):
                status, _, body = get_export(self.base_url, dataset_id=invalid)
                self.assertEqual(status, 422)
                self.assertIn("dataset_id", json.loads(body)["detail"])

    def test_invalid_params_return_422(self):
        cases = [
            {"format": "yaml"},
            {"format": ""},
            {"limit": "0"},
            {"limit": "101"},
            {"limit": "-1"},
            {"limit": "abc"},
            {"limit": ""},
            {"limit": "50.5"},
            {"from": "not-a-time"},
            {"to": "2026-13-01T00:00:00Z"},
            {"from": "2026-02-01T00:00:00Z", "to": "2026-01-01T00:00:00Z"},
            {"from": "2026-01-01T00:00:00Z", "to": "2026-01-01T00:00:00Z"},
        ]
        for params in cases:
            with self.subTest(params=params):
                status, _, _ = get_export(self.base_url, **params)
                self.assertEqual(status, 422)

    def test_limit_truncates_to_first_rows(self):
        with running_demo(0) as base_url:
            for index in range(3):
                seed_release(base_url, f"exp-limit-{index}")
            status, _, body = get_export(base_url, limit=2)
            records = json.loads(body)
            self.assertEqual(status, 200)
            self.assertEqual(len(records), 2)
            self.assertEqual(
                [record["request_id"] for record in records],
                ["exp-limit-2", "exp-limit-1"],
            )

    def test_time_window_inclusive_from_exclusive_to(self):
        with running_demo(0) as base_url:
            record = seed_release(base_url, "exp-window")
            created_at = record["created_at"]
            _, _, body = get_export(base_url, request_id="exp-window", **{"from": created_at})
            self.assertEqual(len(json.loads(body)), 1)
            _, _, body = get_export(base_url, request_id="exp-window", to=created_at)
            self.assertEqual(json.loads(body), [])
            # Naive timestamp is read as UTC; offset timestamps are normalized
            # to UTC before comparison.
            _, _, body = get_export(
                base_url,
                request_id="exp-window",
                **{"from": created_at.replace("+00:00", "")},
            )
            self.assertEqual(len(json.loads(body)), 1)
            # Wall-clock value with +08:00 is 8h earlier as a UTC instant, so
            # the row is included; with -08:00 it is 8h later and excluded.
            _, _, body = get_export_raw(
                base_url,
                "request_id=exp-window&from=" + created_at.replace("+00:00", "%2B08:00"),
            )
            self.assertEqual(len(json.loads(body)), 1)
            _, _, body = get_export_raw(
                base_url,
                "request_id=exp-window&from=" + created_at.replace("+00:00", "-08:00"),
            )
            self.assertEqual(json.loads(body), [])

    def test_export_is_read_only(self):
        with running_demo(0) as base_url:
            seed_release(base_url, "exp-readonly")
            with urlopen(base_url + "/api/privacy-budget", timeout=10) as response:
                budget_before = json.load(response)
            with urlopen(base_url + "/api/releases", timeout=10) as response:
                history_before = len(json.load(response))
            for params in (
                {"format": "json", "limit": 100},
                {"format": "csv", "request_id": "exp-readonly"},
                {"format": "json", "request_id": "missing"},
            ):
                status, _, _ = get_export(base_url, **params)
                self.assertEqual(status, 200)
            with urlopen(base_url + "/api/privacy-budget", timeout=10) as response:
                budget_after = json.load(response)
            with urlopen(base_url + "/api/releases", timeout=10) as response:
                history_after = len(json.load(response))
            self.assertEqual(budget_before, budget_after)
            self.assertEqual(history_before, history_after)


def get_export_raw(base_url, raw_query):
    url = f"{base_url}/api/releases/export?{raw_query}"
    with urlopen(url, timeout=10) as response:
        return response.status, response.headers.get("Content-Type"), response.read().decode("utf-8")


class ExportDatabaseTests(unittest.TestCase):
    def _insert(self, connection, request_id, created_at, dataset_id=DATASET_ID):
        connection.execute(
            "INSERT INTO releases (release_id, request_id, dataset_id, "
            "filters_json, epsilon, published_count, remaining_budget, "
            "created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                f"rel-{request_id}",
                request_id,
                dataset_id,
                "{}",
                0.1,
                3,
                2.9,
                created_at,
            ),
        )

    def test_filters_ordering_limit_and_time_window(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.sqlite3"
            initialize_database(path)
            with closing(sqlite3.connect(path)) as connection, connection:
                self._insert(connection, "old", "2026-01-01T00:00:00.000+00:00")
                self._insert(connection, "mid", "2026-02-01T12:00:00.000+00:00")
                self._insert(connection, "new", "2026-03-01T08:30:00.500+00:00")
                self._insert(
                    connection, "other-set", "2026-02-01T12:00:00.000+00:00",
                    dataset_id="other-dataset",
                )

            rows = export_releases(path, limit=100)
            self.assertEqual(
                [row["request_id"] for row in rows],
                ["new", "other-set", "mid", "old"],
            )

            rows = export_releases(path, dataset_id=DATASET_ID, limit=100)
            self.assertEqual(
                [row["request_id"] for row in rows], ["new", "mid", "old"]
            )

            rows = export_releases(path, request_id="mid", limit=100)
            self.assertEqual([row["request_id"] for row in rows], ["mid"])

            from datetime import datetime, timezone
            from datetime import timedelta

            start = datetime(2026, 2, 1, 12, 0, 0, tzinfo=timezone.utc)
            end = datetime(2026, 3, 1, 8, 30, 0, 500000, tzinfo=timezone.utc)
            rows = export_releases(path, start=start, end=end, limit=100)
            self.assertEqual(
                [row["request_id"] for row in rows], ["other-set", "mid"]
            )

            # Sub-millisecond precision is applied after the widened SQL
            # bound: an inclusive start 500us after the row excludes it, even
            # though the coarse SQL bound (floored to .500) would admit it.
            rows = export_releases(
                path,
                start=datetime(2026, 3, 1, 8, 30, 0, 500500, tzinfo=timezone.utc),
                limit=100,
            )
            self.assertNotIn("new", [row["request_id"] for row in rows])
            # An exclusive end 500us after the row still includes it.
            rows = export_releases(
                path,
                end=datetime(2026, 3, 1, 8, 30, 0, 500500, tzinfo=timezone.utc),
                limit=100,
            )
            self.assertEqual(
                [row["request_id"] for row in rows][0], "new"
            )

            # A timezone-offset bound is compared in UTC.
            rows = export_releases(
                path,
                start=datetime(2026, 2, 1, 20, 0, 0, tzinfo=timezone(timedelta(hours=8))),
                limit=100,
            )
            self.assertEqual(
                [row["request_id"] for row in rows],
                ["new", "other-set", "mid"],
            )

            self.assertEqual(
                [row["request_id"] for row in export_releases(path, limit=2)],
                ["new", "other-set"],
            )

    def test_never_reads_member_table_or_writes(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.sqlite3"
            initialize_database(path)
            with closing(sqlite3.connect(path)) as connection, connection:
                self._insert(connection, "solo", "2026-01-01T00:00:00.000")
                before = list(connection.iterdump())
            rows = export_releases(path)
            self.assertEqual([row["request_id"] for row in rows], ["solo"])
            self.assertEqual(set(rows[0]), PUBLIC_FIELDS)
            with closing(sqlite3.connect(path)) as connection, connection:
                self.assertEqual(list(connection.iterdump()), before)


if __name__ == "__main__":
    unittest.main()
