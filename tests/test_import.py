"""Checks for the run-import and show-batch subcommands."""

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
        self.db_path = Path(self.tmp.name) / "import_ledger.db"
        self.csv_path = Path(self.tmp.name) / "orders.csv"

    def invoke(self, *arguments: str) -> subprocess.CompletedProcess[str]:
        env = dict(os.environ, IMPORT_LEDGER_DB=str(self.db_path))
        return subprocess.run(
            [sys.executable, "-m", "import_pipeline", *arguments],
            cwd=ROOT,
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )

    def write_csv(self, text: str) -> None:
        self.csv_path.write_text(text, encoding="utf-8")

    def register_orders(self) -> None:
        result = self.invoke(
            "add-source", "orders", str(self.csv_path), "id", "amount"
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(
            self.invoke("add-mapping", "orders", "order_id", "id").returncode, 0
        )
        self.assertEqual(
            self.invoke("add-mapping", "orders", "total", "amount").returncode, 0
        )

    def batches(self) -> list[tuple[int, str, int, int]]:
        conn = sqlite3.connect(self.db_path)
        try:
            return conn.execute(
                "SELECT batch_no, status, succeeded_rows, quarantined_rows"
                " FROM batches ORDER BY batch_no"
            ).fetchall()
        finally:
            conn.close()

    def imported_row_count(self) -> int:
        conn = sqlite3.connect(self.db_path)
        try:
            return conn.execute("SELECT COUNT(*) FROM imported_rows").fetchone()[0]
        finally:
            conn.close()

    def test_successful_import_records_ok_batch(self) -> None:
        self.register_orders()
        self.write_csv("order_id,total\n1,10\n2,20\n")
        result = self.invoke("run-import", "orders")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "Result: run-import orders 2")
        self.assertEqual(result.stderr, "")
        self.assertEqual(self.batches(), [(1, "ok", 2, 0)])
        self.assertEqual(self.imported_row_count(), 2)

    def test_missing_csv_file_leaves_no_batch(self) -> None:
        self.register_orders()
        result = self.invoke("run-import", "orders")
        self.assertEqual(result.returncode, 1)
        self.assertTrue(result.stderr.startswith("Error:"))
        self.assertEqual(len(result.stderr.strip().splitlines()), 1)
        self.assertEqual(result.stdout, "")
        self.assertEqual(self.batches(), [])

    def test_unknown_source_is_rejected_without_batch(self) -> None:
        result = self.invoke("run-import", "missing")
        self.assertEqual(result.returncode, 1)
        self.assertTrue(result.stderr.startswith("Error:"))

    def test_unmapped_source_column_rejects_batch(self) -> None:
        self.register_orders()
        self.write_csv("order_id,total,extra\n1,10,x\n")
        result = self.invoke("run-import", "orders")
        self.assertEqual(result.returncode, 1)
        self.assertTrue(result.stderr.startswith("Error:"))
        self.assertEqual(self.batches(), [(1, "rejected", 0, 1)])
        self.assertEqual(self.imported_row_count(), 0)

    def test_missing_target_column_rejects_batch(self) -> None:
        self.assertEqual(
            self.invoke(
                "add-source", "orders", str(self.csv_path), "id", "amount"
            ).returncode,
            0,
        )
        self.assertEqual(
            self.invoke("add-mapping", "orders", "order_id", "id").returncode, 0
        )
        self.write_csv("order_id\n1\n")
        result = self.invoke("run-import", "orders")
        self.assertEqual(result.returncode, 1)
        self.assertTrue(result.stderr.startswith("Error:"))
        self.assertEqual(self.batches(), [(1, "rejected", 0, 1)])
        self.assertEqual(self.imported_row_count(), 0)

    def test_blank_value_rejects_batch(self) -> None:
        self.register_orders()
        self.write_csv("order_id,total\n1,10\n2,  \n")
        result = self.invoke("run-import", "orders")
        self.assertEqual(result.returncode, 1)
        self.assertTrue(result.stderr.startswith("Error:"))
        self.assertEqual(self.batches(), [(1, "rejected", 0, 2)])
        self.assertEqual(self.imported_row_count(), 0)

    def test_repeated_imports_increment_batch_numbers(self) -> None:
        self.register_orders()
        self.write_csv("order_id,total\n1,10\n")
        self.assertEqual(self.invoke("run-import", "orders").returncode, 0)
        self.write_csv("order_id,total\n1,10\n2,\n")
        self.assertEqual(self.invoke("run-import", "orders").returncode, 1)
        self.write_csv("order_id,total\n3,30\n4,40\n")
        self.assertEqual(self.invoke("run-import", "orders").returncode, 0)
        self.assertEqual(
            self.batches(),
            [(1, "ok", 1, 0), (2, "rejected", 0, 2), (3, "ok", 2, 0)],
        )
        self.assertEqual(self.imported_row_count(), 3)

    def test_show_batch_outputs_batches_ascending(self) -> None:
        self.register_orders()
        self.write_csv("order_id,total\n1,10\n")
        self.invoke("run-import", "orders")
        self.write_csv("order_id,total\n1,\n")
        self.invoke("run-import", "orders")
        self.invoke("run-import", "orders")

        result = self.invoke("show-batch", "orders", "1")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(
            result.stdout.splitlines(),
            ["1\tok\t1\t0", "2\trejected\t0\t1", "3\trejected\t0\t1"],
        )

        result = self.invoke("show-batch", "orders", "2")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(
            result.stdout.splitlines(), ["2\trejected\t0\t1", "3\trejected\t0\t1"]
        )

    def test_show_batch_unknown_source_or_batch_is_rejected(self) -> None:
        self.register_orders()
        self.write_csv("order_id,total\n1,10\n")
        self.invoke("run-import", "orders")

        result = self.invoke("show-batch", "missing", "1")
        self.assertEqual(result.returncode, 1)
        self.assertTrue(result.stderr.startswith("Error:"))
        self.assertEqual(result.stdout, "")

        result = self.invoke("show-batch", "orders", "9")
        self.assertEqual(result.returncode, 1)
        self.assertTrue(result.stderr.startswith("Error:"))
        self.assertEqual(result.stdout, "")


if __name__ == "__main__":
    unittest.main()
