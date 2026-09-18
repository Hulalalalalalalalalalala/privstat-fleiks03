"""SQLite storage for the bundled synthetic data catalog."""

import csv
import json
import sqlite3
from contextlib import closing
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
FIELDS = ["member_id", "region", "membership", "age_band", "visit_bucket"]


def initialize_database(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with closing(sqlite3.connect(path)) as connection, connection:
        connection.execute(
            "CREATE TABLE IF NOT EXISTS datasets ("
            "id TEXT PRIMARY KEY, name TEXT NOT NULL, description TEXT NOT NULL, "
            "synthetic INTEGER NOT NULL, fields_json TEXT NOT NULL)"
        )
        connection.execute(
            "CREATE TABLE IF NOT EXISTS retail_members ("
            "member_id TEXT PRIMARY KEY, region TEXT NOT NULL, "
            "membership TEXT NOT NULL, age_band TEXT NOT NULL, "
            "visit_bucket TEXT NOT NULL)"
        )
        if connection.execute(
            "SELECT 1 FROM datasets WHERE id = ?", ("retail-demo",)
        ).fetchone():
            return
        with (ROOT / "data" / "retail_members.csv").open(
            encoding="utf-8", newline=""
        ) as source:
            rows = list(csv.DictReader(source))
        connection.executemany(
            "INSERT INTO retail_members VALUES (?, ?, ?, ?, ?)",
            [tuple(row[field] for field in FIELDS) for row in rows],
        )
        connection.execute(
            "INSERT INTO datasets VALUES (?, ?, ?, ?, ?)",
            (
                "retail-demo",
                "合成零售会员数据",
                "固定构造的区域、会员等级、年龄段和到店频率样例。",
                1,
                json.dumps(FIELDS),
            ),
        )


def list_datasets(path: Path) -> list[dict]:
    with closing(sqlite3.connect(path)) as connection:
        connection.row_factory = sqlite3.Row
        records = connection.execute(
            "SELECT id, name, description, synthetic, fields_json "
            "FROM datasets ORDER BY id"
        ).fetchall()
    return [
        {
            "id": row["id"],
            "name": row["name"],
            "description": row["description"],
            "synthetic": bool(row["synthetic"]),
            "fields": json.loads(row["fields_json"]),
        }
        for row in records
    ]
