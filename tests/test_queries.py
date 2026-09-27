"""Checks for the show-rows and find-dupes read-only subcommands."""

from pathlib import Path
import os
import sqlite3
import subprocess
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]


class QueryCommandTests(unittest.TestCase):
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

    def import_ok_batch(self, text: str) -> None:
        self.write_csv(text)
        result = self.invoke("run-import", "orders")
        self.assertEqual(result.returncode, 0, result.stderr)

    def db_mtime_and_bytes(self) -> tuple[int, bytes]:
        return (
            self.db_path.stat().st_mtime_ns,
            self.db_path.read_bytes(),
        )

    def test_show_rows_outputs_normalized_json_sorted_by_row_number(self) -> None:
        self.register_orders()
        self.import_ok_batch("order_id,total\nA2,30\nA1,10\n")
        result = self.invoke("show-rows", "orders", "1")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stderr, "")
        self.assertEqual(
            result.stdout.splitlines(),
            [
                '1\t{"amount": "30", "id": "A2"}',
                '2\t{"amount": "10", "id": "A1"}',
            ],
        )

    def test_show_rows_rejected_batch_is_empty(self) -> None:
        self.register_orders()
        self.write_csv("order_id,total\nA1,\n")
        self.assertEqual(self.invoke("run-import", "orders").returncode, 1)
        result = self.invoke("show-rows", "orders", "1")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout, "")
        self.assertEqual(result.stderr, "")

    def test_show_rows_revoked_batch_is_empty(self) -> None:
        self.register_orders()
        self.import_ok_batch("order_id,total\nA1,10\n")
        self.assertEqual(self.invoke("revoke-batch", "orders", "1").returncode, 0)
        result = self.invoke("show-rows", "orders", "1")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout, "")

    def test_show_rows_rejects_unknown_source_and_batch(self) -> None:
        self.register_orders()
        self.import_ok_batch("order_id,total\nA1,10\n")
        for arguments in [
            ("show-rows", "missing", "1"),
            ("show-rows", "orders", "9"),
            ("show-rows", "orders", "0"),
            ("show-rows", "orders", "-1"),
            ("show-rows", "OK", "1"),
        ]:
            with self.subTest(arguments=arguments):
                before = self.db_mtime_and_bytes()
                result = self.invoke(*arguments)
                self.assertEqual(result.returncode, 1)
                self.assertEqual(result.stdout, "")
                self.assertTrue(result.stderr.startswith("Error:"))
                self.assertEqual(len(result.stderr.strip().splitlines()), 1)
                self.assertEqual(self.db_mtime_and_bytes(), before)

    def test_find_dupes_merges_batches_and_shrinks_after_revoke(self) -> None:
        self.register_orders()
        self.import_ok_batch("order_id,total\nA1,10\nA2,20\n")
        self.import_ok_batch("order_id,total\nA1,30\nA3,20\n")
        result = self.invoke("find-dupes", "orders", "id")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.splitlines(), ["A1\t2\t1,2"])
        # amount 列：20 跨两批重复
        result = self.invoke("find-dupes", "orders", "amount")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.splitlines(), ["20\t2\t1,2"])
        # 撤销批次 1 后，其行不再参与统计
        self.assertEqual(self.invoke("revoke-batch", "orders", "1").returncode, 0)
        result = self.invoke("find-dupes", "orders", "id")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout, "")
        # show-rows 对未撤销批次不受影响
        result = self.invoke("show-rows", "orders", "2")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(
            result.stdout.splitlines(),
            ['1\t{"amount": "30", "id": "A1"}', '2\t{"amount": "20", "id": "A3"}'],
        )

    def test_find_dupes_same_batch_duplicates(self) -> None:
        self.register_orders()
        self.import_ok_batch("order_id,total\nA1,10\nA1,20\nA2,10\n")
        result = self.invoke("find-dupes", "orders", "id")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.splitlines(), ["A1\t2\t1"])

    def test_find_dupes_rejects_invalid_arguments(self) -> None:
        self.register_orders()
        self.import_ok_batch("order_id,total\nA1,10\n")
        for arguments in [
            ("find-dupes", "missing", "id"),
            ("find-dupes", "orders", "nosuchcolumn"),
            ("find-dupes", "orders", ""),
            ("find-dupes", "OK", "id"),
        ]:
            with self.subTest(arguments=arguments):
                before = self.db_mtime_and_bytes()
                result = self.invoke(*arguments)
                self.assertEqual(result.returncode, 1)
                self.assertEqual(result.stdout, "")
                self.assertTrue(result.stderr.startswith("Error:"))
                self.assertEqual(len(result.stderr.strip().splitlines()), 1)
                self.assertEqual(self.db_mtime_and_bytes(), before)

    def test_queries_do_not_modify_database(self) -> None:
        self.register_orders()
        self.import_ok_batch("order_id,total\nA1,10\nA1,20\n")
        before = self.db_mtime_and_bytes()
        self.assertEqual(self.invoke("show-rows", "orders", "1").returncode, 0)
        self.assertEqual(self.invoke("find-dupes", "orders", "id").returncode, 0)
        self.assertEqual(self.db_mtime_and_bytes(), before)


if __name__ == "__main__":
    unittest.main()
