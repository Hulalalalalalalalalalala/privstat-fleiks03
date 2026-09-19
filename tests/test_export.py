import csv
import io
import json
import unittest
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import urlopen

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


def post_release(base_url, payload):
    from urllib.request import Request

    request = Request(
        base_url + "/api/releases",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urlopen(request, timeout=10) as response:
        return response.status, json.loads(response.read())


def get_export(base_url, **parameters):
    query = urlencode(parameters)
    url = base_url + "/api/releases/export" + (f"?{query}" if query else "")
    try:
        with urlopen(url, timeout=10) as response:
            headers = {key.lower(): value for key, value in response.headers.items()}
            return response.status, headers, response.read().decode("utf-8")
    except HTTPError as error:
        body = error.read().decode("utf-8")
        status = error.code
        error.close()
        return status, {}, body


def publish(base_url, request_id, *, filters=None, epsilon=0.2):
    status, record = post_release(
        base_url,
        {
            "request_id": request_id,
            "dataset_id": "retail-demo",
            "filters": filters,
            "epsilon": epsilon,
        },
    )
    assert status in (200, 201)
    return record


class ExportJsonTests(unittest.TestCase):
    def test_default_json_returns_only_public_fields_newest_first(self):
        with running_demo(0) as base_url:
            first = publish(base_url, "exp-json-1", filters={"region": "north"})
            second = publish(base_url, "exp-json-2", filters={"region": "south"})
            status, headers, body = get_export(base_url)
            self.assertEqual(status, 200)
            self.assertTrue(headers.get("content-type", "").startswith("application/json"))
            records = json.loads(body)
            self.assertEqual([r["request_id"] for r in records[:2]], ["exp-json-2", "exp-json-1"])
            self.assertGreaterEqual(records[0]["created_at"], second["created_at"])
            for record in records:
                self.assertEqual(set(record), PUBLIC_FIELDS)
            serialized = body
            self.assertNotIn("member_id", serialized)
            self.assertNotIn("S001", serialized)
            self.assertNotIn("true_count", serialized)
            self.assertEqual(first["release_id"], records[1]["release_id"])

    def test_limit_caps_results(self):
        with running_demo(0) as base_url:
            publish(base_url, "exp-limit-1")
            publish(base_url, "exp-limit-2")
            publish(base_url, "exp-limit-3")
            status, _, body = get_export(base_url, limit=2)
            self.assertEqual(status, 200)
            records = json.loads(body)
            self.assertEqual(len(records), 2)
            self.assertEqual(
                [r["request_id"] for r in records], ["exp-limit-3", "exp-limit-2"]
            )

    def test_dataset_filter(self):
        with running_demo(0) as base_url:
            publish(base_url, "exp-dataset-1")
            status, _, body = get_export(base_url, dataset_id="retail-demo")
            self.assertEqual(status, 200)
            self.assertEqual(len(json.loads(body)), 1)
            self.assertEqual(json.loads(body)[0]["request_id"], "exp-dataset-1")

    def test_request_id_exact_match_and_empty_result(self):
        with running_demo(0) as base_url:
            publish(base_url, "exp-exact-1")
            publish(base_url, "exp-exact-2")
            status, _, body = get_export(base_url, request_id="exp-exact-1")
            self.assertEqual(status, 200)
            records = json.loads(body)
            self.assertEqual(len(records), 1)
            self.assertEqual(records[0]["request_id"], "exp-exact-1")
            # A missing request_id is a successful empty result, not an error.
            status, _, body = get_export(base_url, request_id="exp-does-not-exist")
            self.assertEqual(status, 200)
            self.assertEqual(json.loads(body), [])

    def test_time_window_is_from_inclusive_to_exclusive(self):
        with running_demo(0) as base_url:
            earlier = publish(base_url, "exp-time-1")
            later = publish(base_url, "exp-time-2")
            cut = later["created_at"]
            status, _, body = get_export(base_url, **{"from": cut})
            self.assertEqual(status, 200)
            ids = {r["request_id"] for r in json.loads(body)}
            self.assertEqual(ids, {"exp-time-2"})
            # to is exclusive: a record stamped exactly at `to` is excluded.
            status, _, body = get_export(base_url, **{"to": cut})
            self.assertEqual(status, 200)
            self.assertEqual(
                {r["request_id"] for r in json.loads(body)}, {"exp-time-1"}
            )
            status, _, body = get_export(
                base_url, **{"from": earlier["created_at"], "to": cut}
            )
            self.assertEqual(status, 200)
            self.assertEqual(
                {r["request_id"] for r in json.loads(body)}, {"exp-time-1"}
            )

    def test_export_is_read_only(self):
        with running_demo(0) as base_url:
            publish(base_url, "exp-readonly-1")
            with urlopen(base_url + "/api/privacy-budget", timeout=10) as response:
                budget_before = json.load(response)
            with urlopen(base_url + "/api/releases", timeout=10) as response:
                history_before = json.load(response)
            for kwargs in ({}, {"format": "csv"}, {"request_id": "exp-readonly-1"}):
                self.assertEqual(get_export(base_url, **kwargs)[0], 200)
            with urlopen(base_url + "/api/privacy-budget", timeout=10) as response:
                budget_after = json.load(response)
            with urlopen(base_url + "/api/releases", timeout=10) as response:
                history_after = json.load(response)
            self.assertEqual(budget_before, budget_after)
            self.assertEqual(history_before, history_after)


class ExportCsvTests(unittest.TestCase):
    def test_csv_has_fixed_header_and_normalized_filters(self):
        with running_demo(0) as base_url:
            publish(
                base_url,
                "exp-csv-1",
                filters={"membership": "plus", "region": "north"},
            )
            status, headers, body = get_export(base_url, format="csv")
            self.assertEqual(status, 200)
            self.assertIn("text/csv", headers.get("content-type", ""))
            self.assertIn("attachment", headers.get("content-disposition", ""))
            self.assertEqual(body.splitlines()[0], ",".join(CSV_HEADER))
            rows = list(csv.reader(io.StringIO(body)))
            self.assertEqual(rows[0], CSV_HEADER)
            row = rows[1]
            record = dict(zip(CSV_HEADER, row))
            self.assertEqual(record["request_id"], "exp-csv-1")
            self.assertEqual(record["dataset_id"], "retail-demo")
            # Filters are a normalized JSON string: sorted keys, no spaces.
            self.assertEqual(
                record["filters"], '{"membership":"plus","region":"north"}'
            )
            self.assertEqual(float(record["epsilon"]), 0.2)
            self.assertGreaterEqual(int(record["published_count"]), 0)

    def test_csv_empty_result_is_header_only(self):
        with running_demo(0) as base_url:
            publish(base_url, "exp-csv-empty")
            status, _, body = get_export(
                base_url, format="csv", request_id="no-such-request"
            )
            self.assertEqual(status, 200)
            lines = body.strip().splitlines()
            self.assertEqual(lines, [",".join(CSV_HEADER)])


class ExportValidationTests(unittest.TestCase):
    def test_invalid_parameters_return_422(self):
        with running_demo(0) as base_url:
            publish(base_url, "exp-422")
            cases = [
                {"format": "xml"},
                {"format": "CSV"},
                {"dataset_id": "other-dataset"},
                {"dataset_id": "retail-demo "},
                {"request_id": "  "},
                {"limit": 0},
                {"limit": 101},
                {"limit": "abc"},
                {"from": "not-a-time"},
                {"to": "2026-13-40T99:99:99Z"},
            ]
            for parameters in cases:
                with self.subTest(parameters=parameters):
                    status, _, _ = get_export(base_url, **parameters)
                    self.assertEqual(status, 422)

    def test_from_not_before_to_returns_422(self):
        with running_demo(0) as base_url:
            cut = "2026-01-01T00:00:00Z"
            self.assertEqual(
                get_export(base_url, **{"from": cut, "to": cut})[0], 422
            )
            self.assertEqual(
                get_export(
                    base_url, **{"from": "2026-02-01T00:00:00Z", "to": cut}
                )[0],
                422,
            )


if __name__ == "__main__":
    unittest.main()
