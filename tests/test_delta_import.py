"""Checks for the delta-import and list-quarantine subcommands."""

from pathlib import Path
import os
import sqlite3
import subprocess
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]


class DeltaImportCommandTests(unittest.TestCase):
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

    def test_first_import_inserts_all_rows(self) -> None:
        self.register_orders()
        self.write_csv("order_id,total\n1,10\n2,20\n")
        result = self.invoke("delta-import", "orders")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "Result: delta-import orders 2 0 0")
        self.assertEqual(result.stderr, "")
        self.assertEqual(self.batches(), [(1, "ok", 2, 0)])
        self.assertEqual(self.imported_row_count(), 2)

    def test_rerun_skips_known_keys(self) -> None:
        self.register_orders()
        self.write_csv("order_id,total\n1,10\n2,20\n")
        self.assertEqual(self.invoke("delta-import", "orders").returncode, 0)
        self.write_csv("order_id,total\n1,99\n2,88\n3,30\n")
        result = self.invoke("delta-import", "orders")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "Result: delta-import orders 1 2 0")
        self.assertEqual(self.batches(), [(1, "ok", 2, 0), (2, "ok", 1, 0)])
        self.assertEqual(self.imported_row_count(), 3)

    def test_business_key_matches_exactly(self) -> None:
        self.register_orders()
        self.write_csv("order_id,total\nA1,10\n")
        self.assertEqual(self.invoke("delta-import", "orders").returncode, 0)
        self.write_csv("order_id,total\na1,20\n A1,30\nA1 ,40\n")
        result = self.invoke("delta-import", "orders")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "Result: delta-import orders 3 0 0")
        self.assertEqual(self.imported_row_count(), 4)

    def test_blank_value_rows_are_quarantined(self) -> None:
        self.register_orders()
        self.write_csv("order_id,total\n1,10\n2,  \n,30\n")
        result = self.invoke("delta-import", "orders")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "Result: delta-import orders 1 0 2")
        self.assertEqual(self.batches(), [(1, "failed", 1, 2)])
        self.assertEqual(self.imported_row_count(), 1)

    def test_intra_batch_duplicate_keys_are_quarantined(self) -> None:
        self.register_orders()
        self.write_csv("order_id,total\n1,10\n2,20\n2,25\n3,30\n")
        result = self.invoke("delta-import", "orders")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "Result: delta-import orders 2 0 2")
        self.assertEqual(self.batches(), [(1, "failed", 2, 2)])
        self.assertEqual(self.imported_row_count(), 2)

        result = self.invoke("list-quarantine", "orders")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(
            result.stdout.splitlines(),
            [
                "1\t2\tduplicate-key\tamount=20,id=2",
                "1\t3\tduplicate-key\tamount=25,id=2",
            ],
        )

    def test_quarantine_listing_is_ordered_and_sorted_by_column(self) -> None:
        self.register_orders()
        self.write_csv("order_id,total\n1,10\n,20\n")
        self.assertEqual(self.invoke("delta-import", "orders").returncode, 0)
        self.write_csv("order_id,total\n3,\n4,40\n")
        self.assertEqual(self.invoke("delta-import", "orders").returncode, 0)

        result = self.invoke("list-quarantine", "orders")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(
            result.stdout.splitlines(),
            [
                "1\t2\tblank-value\tamount=20,id=",
                "2\t1\tblank-value\tamount=,id=3",
            ],
        )
        self.assertEqual(
            self.batches(), [(1, "failed", 1, 1), (2, "failed", 1, 1)]
        )

    def test_unmapped_source_column_rejects_batch(self) -> None:
        self.register_orders()
        self.write_csv("order_id,total,extra\n1,10,x\n")
        result = self.invoke("delta-import", "orders")
        self.assertEqual(result.returncode, 1)
        self.assertTrue(result.stderr.startswith("Error:"))
        self.assertEqual(result.stdout, "")
        self.assertEqual(self.batches(), [(1, "rejected", 0, 1)])
        self.assertEqual(self.imported_row_count(), 0)

    def test_missing_required_column_rejects_batch(self) -> None:
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
        result = self.invoke("delta-import", "orders")
        self.assertEqual(result.returncode, 1)
        self.assertTrue(result.stderr.startswith("Error:"))
        self.assertEqual(self.batches(), [(1, "rejected", 0, 1)])
        self.assertEqual(self.imported_row_count(), 0)

    def test_missing_csv_file_leaves_no_batch(self) -> None:
        self.register_orders()
        result = self.invoke("delta-import", "orders")
        self.assertEqual(result.returncode, 1)
        self.assertTrue(result.stderr.startswith("Error:"))
        self.assertEqual(len(result.stderr.strip().splitlines()), 1)
        self.assertEqual(result.stdout, "")
        self.assertEqual(self.batches(), [])

    def test_unknown_source_is_rejected_without_batch(self) -> None:
        for command in ["delta-import", "list-quarantine"]:
            with self.subTest(command=command):
                result = self.invoke(command, "missing")
                self.assertEqual(result.returncode, 1)
                self.assertTrue(result.stderr.startswith("Error:"))
                self.assertEqual(result.stdout, "")
        self.assertEqual(self.batches(), [])

    def test_list_quarantine_empty_when_nothing_quarantined(self) -> None:
        self.register_orders()
        self.write_csv("order_id,total\n1,10\n")
        self.assertEqual(self.invoke("delta-import", "orders").returncode, 0)
        result = self.invoke("list-quarantine", "orders")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout, "")


if __name__ == "__main__":
    unittest.main()
