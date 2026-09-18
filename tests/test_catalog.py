import json
import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import urlopen

from privstat.database import initialize_database
from privstat.demo import running_demo


class CatalogTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.service = running_demo(0)
        cls.base_url = cls.service.__enter__()

    @classmethod
    def tearDownClass(cls):
        cls.service.__exit__(None, None, None)

    def test_public_http_catalog_contains_only_metadata(self):
        with urlopen(self.base_url + "/api/datasets") as response:
            datasets = json.load(response)
        self.assertEqual(len(datasets), 1)
        self.assertEqual(
            set(datasets[0]), {"id", "name", "description", "synthetic", "fields"}
        )
        self.assertTrue(datasets[0]["synthetic"])
        self.assertNotIn("S001", json.dumps(datasets))

    def test_page_and_health_are_available(self):
        with urlopen(self.base_url + "/") as response:
            self.assertIn("PrivStat", response.read().decode("utf-8"))
        with urlopen(self.base_url + "/health") as response:
            self.assertEqual(json.load(response)["status"], "ok")

    def test_source_records_are_not_served(self):
        with self.assertRaises(HTTPError) as error:
            urlopen(self.base_url + "/data/retail_members.csv")
        self.assertEqual(error.exception.code, 404)
        error.exception.close()

    def test_repeated_initialization_preserves_existing_data(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "catalog.sqlite3"
            initialize_database(path)
            with closing(sqlite3.connect(path)) as connection, connection:
                original_count = connection.execute("SELECT COUNT(*) FROM retail_members").fetchone()[0]
                connection.execute("UPDATE retail_members SET region = 'local' WHERE member_id = 'S001'")
            initialize_database(path)
            with closing(sqlite3.connect(path)) as connection:
                self.assertEqual(connection.execute("SELECT COUNT(*) FROM retail_members").fetchone()[0], original_count)
                self.assertEqual(connection.execute("SELECT region FROM retail_members WHERE member_id = 'S001'").fetchone()[0], "local")


if __name__ == "__main__":
    unittest.main()
