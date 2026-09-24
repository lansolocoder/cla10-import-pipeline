"""Checks for the `import <source>` batch-import subcommand."""

from pathlib import Path
import csv
import hashlib
import os
import sqlite3
import subprocess
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]


def expected_signature(mapped: dict[str, str]) -> str:
    payload = "&".join(f"{k}={mapped[k]}" for k in sorted(mapped))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


class ImportCommandTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.workdir = Path(self.tmp.name)
        self.db_path = self.workdir / "import_ledger.db"

    def invoke(self, *arguments: str) -> subprocess.CompletedProcess[str]:
        env = dict(
            os.environ,
            IMPORT_LEDGER_DB=str(self.db_path),
            PYTHONPATH=str(ROOT),
        )
        return subprocess.run(
            [sys.executable, "-m", "import_pipeline", *arguments],
            cwd=self.workdir,
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )

    def write_csv(self, name: str, rows: list[list[str]]) -> Path:
        path = self.workdir / name
        with path.open("w", encoding="utf-8", newline="") as handle:
            csv.writer(handle).writerows(rows)
        return path

    def register_orders(self, csv_name: str = "orders.csv") -> Path:
        path = self.workdir / csv_name
        self.assertEqual(
            self.invoke(
                "add-source", "orders", str(path), "id", "amount"
            ).returncode,
            0,
        )
        self.assertEqual(
            self.invoke("add-mapping", "orders", "order_id", "id").returncode, 0
        )
        self.assertEqual(
            self.invoke("add-mapping", "orders", "total", "amount").returncode, 0
        )
        return path

    def batches(self) -> list[tuple]:
        conn = sqlite3.connect(self.db_path)
        try:
            return conn.execute(
                "SELECT source_name, batch_no, total_rows, success_count,"
                " duplicate_count, failed_count, new_count FROM import_batches"
                " ORDER BY source_name, batch_no"
            ).fetchall()
        finally:
            conn.close()

    def signatures(self, source: str = "orders") -> list[str]:
        conn = sqlite3.connect(self.db_path)
        try:
            return [
                r[0]
                for r in conn.execute(
                    "SELECT signature FROM imported_records"
                    " WHERE source_name = ? ORDER BY id",
                    (source,),
                )
            ]
        finally:
            conn.close()

    def test_first_import_marks_all_rows_as_new(self) -> None:
        self.register_orders()
        self.write_csv(
            "orders.csv",
            [
                ["order_id", "total"],
                ["o1", "100"],
                ["o2", "200"],
            ],
        )
        result = self.invoke("import", "orders")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(
            result.stdout.strip(), "Result: import orders 1 2 2 0 0 2"
        )
        self.assertEqual(
            self.batches(), [("orders", 1, 2, 2, 0, 0, 2)]
        )
        self.assertEqual(
            self.signatures(),
            [
                expected_signature({"id": "o1", "amount": "100"}),
                expected_signature({"id": "o2", "amount": "200"}),
            ],
        )
        self.assertEqual(
            list(self.workdir.glob("rejected_*.csv")), []
        )

    def test_reimport_identical_file_is_all_duplicates(self) -> None:
        self.register_orders()
        self.write_csv(
            "orders.csv",
            [["order_id", "total"], ["o1", "100"], ["o2", "200"]],
        )
        first = self.invoke("import", "orders")
        self.assertEqual(first.returncode, 0, first.stderr)

        result = self.invoke("import", "orders")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(
            result.stdout.strip(), "Result: import orders 2 2 0 2 0 0"
        )
        self.assertEqual(
            self.batches(),
            [
                ("orders", 1, 2, 2, 0, 0, 2),
                ("orders", 2, 2, 0, 2, 0, 0),
            ],
        )

    def test_partial_import_counts_new_duplicate_and_failed(self) -> None:
        self.register_orders()
        self.write_csv(
            "orders.csv",
            [["order_id", "total"], ["o1", "100"], ["o2", "200"]],
        )
        self.assertEqual(self.invoke("import", "orders").returncode, 0)

        self.write_csv(
            "orders.csv",
            [
                ["order_id", "total"],
                ["o1", "100"],      # 与批次 1 重复
                ["o3", "300"],      # 增量新数据
                ["o4", ""],         # 必需字段为空 -> 失败
                ["o5", "500", "x"], # 列数不一致 -> 失败
            ],
        )
        result = self.invoke("import", "orders")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(
            result.stdout.strip(), "Result: import orders 2 4 1 1 2 1"
        )
        rejected = self.workdir / "rejected_orders_2.csv"
        self.assertTrue(rejected.is_file())
        with rejected.open(encoding="utf-8", newline="") as handle:
            rows = list(csv.reader(handle))
        self.assertEqual(
            rows,
            [
                ["order_id", "total"],
                ["o4", ""],
                ["o5", "500", "x"],
            ],
        )

    def test_rejected_rows_keep_file_order_and_original_columns(self) -> None:
        self.register_orders()
        self.write_csv(
            "orders.csv",
            [
                ["order_id", "total"],
                ["o5", "500", "extra"],  # 多出的列在前
                ["", "100"],             # 缺 id
                ["o6", "600"],           # 成功
                ["o7", ""],              # 空金额在后
            ],
        )
        result = self.invoke("import", "orders")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(
            result.stdout.strip(), "Result: import orders 1 4 1 0 3 1"
        )
        with (self.workdir / "rejected_orders_1.csv").open(
            encoding="utf-8", newline=""
        ) as handle:
            rows = list(csv.reader(handle))
        self.assertEqual(
            rows,
            [
                ["order_id", "total"],
                ["o5", "500", "extra"],
                ["", "100"],
                ["o7", ""],
            ],
        )

    def test_empty_file_without_header_results_in_zero_counts(self) -> None:
        self.register_orders()
        (self.workdir / "orders.csv").write_text("", encoding="utf-8")
        result = self.invoke("import", "orders")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(
            result.stdout.strip(), "Result: import orders 1 0 0 0 0 0"
        )
        self.assertEqual(list(self.workdir.glob("rejected_*.csv")), [])

    def test_missing_required_header_column_quarantines_rows(self) -> None:
        self.register_orders()
        # 表头缺少 order_id（必需字段 id 的来源列），数据行列数与表头一致
        self.write_csv(
            "orders.csv",
            [["total"], ["100"], ["200"]],
        )
        result = self.invoke("import", "orders")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(
            result.stdout.strip(), "Result: import orders 1 2 0 0 2 0"
        )
        with (self.workdir / "rejected_orders_1.csv").open(
            encoding="utf-8", newline=""
        ) as handle:
            rows = list(csv.reader(handle))
        self.assertEqual(rows, [["total"], ["100"], ["200"]])

    def test_values_are_used_raw_without_trim_or_case_change(self) -> None:
        self.register_orders()
        self.write_csv(
            "orders.csv",
            [["order_id", "total"], [" O1 ", " ONERED "]],
        )
        result = self.invoke("import", "orders")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(
            self.signatures(),
            [expected_signature({"id": " O1 ", "amount": " ONERED "})],
        )

    def test_unknown_source_is_rejected_without_side_effects(self) -> None:
        result = self.invoke("import", "ghost")
        self.assertEqual(result.returncode, 1)
        self.assertIn("ghost", result.stderr)
        self.assertNotIn("Result:", result.stdout)
        self.assertEqual(list(self.workdir.glob("rejected_*.csv")), [])

    def test_missing_csv_is_rejected_without_side_effects(self) -> None:
        self.register_orders("missing.csv")  # 不实际创建文件
        result = self.invoke("import", "orders")
        self.assertEqual(result.returncode, 1)
        self.assertNotEqual(result.stderr, "")
        self.assertEqual(self.batches(), [])
        self.assertEqual(list(self.workdir.glob("rejected_*.csv")), [])

    def test_unmapped_csv_column_is_rejected_without_side_effects(self) -> None:
        self.register_orders()
        self.write_csv(
            "orders.csv",
            [["order_id", "total", "note"], ["o1", "100", "hi"]],
        )
        result = self.invoke("import", "orders")
        self.assertEqual(result.returncode, 1)
        self.assertIn("note", result.stderr)
        self.assertEqual(self.batches(), [])
        self.assertEqual(list(self.workdir.glob("rejected_*.csv")), [])

    def test_unmapped_required_field_is_rejected_without_side_effects(self) -> None:
        path = self.workdir / "people.csv"
        self.assertEqual(
            self.invoke(
                "add-source", "people", str(path), "id", "email"
            ).returncode,
            0,
        )
        # 只映射 id，必需字段 email 未映射
        self.assertEqual(
            self.invoke("add-mapping", "people", "pid", "id").returncode, 0
        )
        self.write_csv(
            "people.csv",
            [["pid", "mail"], ["p1", "a@example.com"]],
        )
        result = self.invoke("import", "people")
        self.assertEqual(result.returncode, 1)
        self.assertIn("email", result.stderr)
        self.assertEqual(self.batches(), [])
        self.assertEqual(list(self.workdir.glob("rejected_*.csv")), [])

    def test_batch_numbers_increment_per_source(self) -> None:
        self.register_orders()
        self.write_csv(
            "orders.csv", [["order_id", "total"], ["o1", "100"]]
        )
        self.assertEqual(self.invoke("import", "orders").returncode, 0)
        self.assertEqual(self.invoke("import", "orders").returncode, 0)

        other = self.workdir / "other.csv"
        self.assertEqual(
            self.invoke("add-source", "other", str(other), "k").returncode, 0
        )
        self.assertEqual(
            self.invoke("add-mapping", "other", "key", "k").returncode, 0
        )
        self.write_csv("other.csv", [["key"], ["v1"]])
        result = self.invoke("import", "other")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "Result: import other 1 1 1 0 0 1")

        result = self.invoke("import", "orders")
        self.assertEqual(
            result.stdout.strip(), "Result: import orders 3 1 0 1 0 0"
        )


if __name__ == "__main__":
    unittest.main()
