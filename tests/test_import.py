"""Checks for the batch import subcommand."""

from pathlib import Path
import os
import sqlite3
import subprocess
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]


class ImportCommandTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.work = Path(self.tmp.name)
        self.db_path = self.work / "import_ledger.db"

    def invoke(self, *arguments: str) -> subprocess.CompletedProcess[str]:
        env = dict(
            os.environ,
            IMPORT_LEDGER_DB=str(self.db_path),
            PYTHONPATH=str(ROOT),
        )
        return subprocess.run(
            [sys.executable, "-m", "import_pipeline", *arguments],
            cwd=self.work,
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )

    def write_csv(self, name: str, content: str) -> str:
        path = self.work / name
        path.write_text(content, encoding="utf-8")
        return str(path)

    def register_orders(self, csv_path: str) -> None:
        self.assertEqual(
            self.invoke("add-source", "orders", csv_path, "id", "amount").returncode,
            0,
        )
        self.assertEqual(
            self.invoke("add-mapping", "orders", "order_id", "id").returncode, 0
        )
        self.assertEqual(
            self.invoke("add-mapping", "orders", "total", "amount").returncode, 0
        )

    def batch_rows(self) -> list[tuple]:
        conn = sqlite3.connect(self.db_path)
        try:
            return conn.execute(
                "SELECT source_name, batch_no, total_rows, imported_rows,"
                " duplicate_rows, rejected_rows, incremental_rows"
                " FROM import_batches ORDER BY source_name, batch_no"
            ).fetchall()
        finally:
            conn.close()

    def test_import_success_with_rejected_rows(self) -> None:
        csv_path = self.write_csv(
            "orders.csv",
            "order_id,total\nA1,10\nA2,\nA3,30\n",
        )
        self.register_orders(csv_path)
        result = self.invoke("import", "orders")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "Result: import orders 1 3 2 0 1 2")
        self.assertEqual(result.stderr, "")

        rejected = self.work / "rejected_orders_1.csv"
        self.assertTrue(rejected.is_file())
        self.assertEqual(rejected.read_text(encoding="utf-8"), "order_id,total\nA2,\n")

        self.assertEqual(
            self.batch_rows(), [("orders", 1, 3, 2, 0, 1, 2)]
        )

    def test_reimport_marks_duplicates_and_incremental_rows(self) -> None:
        csv_path = self.write_csv("orders.csv", "order_id,total\nA1,10\nA2,20\n")
        self.register_orders(csv_path)
        self.assertEqual(self.invoke("import", "orders").returncode, 0)

        result = self.invoke("import", "orders")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "Result: import orders 2 2 0 2 0 0")

        Path(csv_path).write_text(
            "order_id,total\nA1,10\nA2,20\nA3,30\n", encoding="utf-8"
        )
        result = self.invoke("import", "orders")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "Result: import orders 3 3 1 2 0 1")
        self.assertFalse((self.work / "rejected_orders_3.csv").exists())

    def test_column_count_mismatch_rows_are_rejected(self) -> None:
        csv_path = self.write_csv(
            "orders.csv",
            "order_id,total\nA1,10\nB2\nC3,30,extra\n",
        )
        self.register_orders(csv_path)
        result = self.invoke("import", "orders")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "Result: import orders 1 3 1 0 2 1")
        rejected = (self.work / "rejected_orders_1.csv").read_text(encoding="utf-8")
        self.assertEqual(rejected.splitlines(), ["order_id,total", "B2", "C3,30,extra"])

    def test_unknown_source_fails_without_side_effects(self) -> None:
        result = self.invoke("import", "missing")
        self.assertEqual(result.returncode, 1)
        self.assertIn("missing", result.stderr)
        self.assertNotIn("Result:", result.stdout)
        self.assertEqual(list(self.work.glob("rejected_*.csv")), [])

    def test_missing_csv_fails_without_batch_record(self) -> None:
        self.assertEqual(
            self.invoke("add-source", "orders", "nope.csv", "id").returncode, 0
        )
        self.assertEqual(
            self.invoke("add-mapping", "orders", "order_id", "id").returncode, 0
        )
        result = self.invoke("import", "orders")
        self.assertEqual(result.returncode, 1)
        self.assertNotEqual(result.stderr, "")
        self.assertEqual(self.batch_rows(), [])

    def test_unmapped_header_column_fails_without_side_effects(self) -> None:
        csv_path = self.write_csv("orders.csv", "order_id,total,extra\nA1,10,x\n")
        self.register_orders(csv_path)
        result = self.invoke("import", "orders")
        self.assertEqual(result.returncode, 1)
        self.assertIn("extra", result.stderr)
        self.assertEqual(self.batch_rows(), [])
        self.assertEqual(list(self.work.glob("rejected_*.csv")), [])

    def test_required_field_not_mapped_fails(self) -> None:
        csv_path = self.write_csv("orders.csv", "order_id,total\nA1,10\n")
        self.assertEqual(
            self.invoke("add-source", "orders", csv_path, "id", "amount").returncode,
            0,
        )
        self.assertEqual(
            self.invoke("add-mapping", "orders", "order_id", "id").returncode, 0
        )
        self.assertEqual(
            self.invoke("add-mapping", "orders", "total", "total").returncode, 0
        )
        result = self.invoke("import", "orders")
        self.assertEqual(result.returncode, 1)
        self.assertIn("amount", result.stderr)
        self.assertEqual(self.batch_rows(), [])

    def test_batch_numbers_increment_per_source(self) -> None:
        csv_path = self.write_csv("orders.csv", "order_id,total\nA1,10\n")
        other_path = self.write_csv("other.csv", "order_id,total\nB1,99\n")
        self.register_orders(csv_path)
        self.assertEqual(
            self.invoke("add-source", "other", other_path, "id").returncode, 0
        )
        self.assertEqual(
            self.invoke("add-mapping", "other", "order_id", "id").returncode, 0
        )
        self.assertEqual(
            self.invoke("add-mapping", "other", "total", "amount").returncode, 0
        )

        self.assertIn(
            "Result: import orders 1 1 1 0 0 1", self.invoke("import", "orders").stdout
        )
        self.assertIn(
            "Result: import other 1 1 1 0 0 1", self.invoke("import", "other").stdout
        )
        self.assertIn(
            "Result: import orders 2 1 0 1 0 0", self.invoke("import", "orders").stdout
        )


if __name__ == "__main__":
    unittest.main()
