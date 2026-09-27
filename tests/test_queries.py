"""Checks for the show-rows and find-dupes read-only subcommands."""

from pathlib import Path
import hashlib
import os
import subprocess
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]


class ReadonlyQueryTests(unittest.TestCase):
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

    def db_digest(self) -> str:
        return hashlib.sha256(self.db_path.read_bytes()).hexdigest()

    def assert_readonly(self, result: subprocess.CompletedProcess[str]) -> None:
        self.assertEqual(result.stderr, "")
        self.assertEqual(self.db_digest(), self.digest_before)

    def import_rows(self, rows: str) -> None:
        self.write_csv(rows)
        result = self.invoke("run-import", "orders")
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_show_rows_outputs_normalized_json_by_row_number(self) -> None:
        self.register_orders()
        self.import_rows("order_id,total\n1,10\n2,20\n")
        self.digest_before = self.db_digest()
        result = self.invoke("show-rows", "orders", "1")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(
            result.stdout.splitlines(),
            [
                '1\t{"amount": "10", "id": "1"}',
                '2\t{"amount": "20", "id": "2"}',
            ],
        )
        self.assert_readonly(result)

    def test_show_rows_rejected_batch_is_empty(self) -> None:
        self.register_orders()
        self.write_csv("order_id,total\n1,\n")
        self.assertEqual(self.invoke("run-import", "orders").returncode, 1)
        self.digest_before = self.db_digest()
        result = self.invoke("show-rows", "orders", "1")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout, "")
        self.assert_readonly(result)

    def test_show_rows_revoked_batch_is_empty(self) -> None:
        self.register_orders()
        self.import_rows("order_id,total\n1,10\n")
        self.assertEqual(self.invoke("revoke-batch", "orders", "1").returncode, 0)
        self.digest_before = self.db_digest()
        result = self.invoke("show-rows", "orders", "1")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout, "")
        self.assert_readonly(result)

    def test_show_rows_other_batch_revoke_does_not_affect_output(self) -> None:
        self.register_orders()
        self.import_rows("order_id,total\n1,10\n2,20\n")
        self.import_rows("order_id,total\n3,30\n")
        self.assertEqual(self.invoke("revoke-batch", "orders", "2").returncode, 0)
        result = self.invoke("show-rows", "orders", "1")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(
            result.stdout.splitlines(),
            ['1\t{"amount": "10", "id": "1"}', '2\t{"amount": "20", "id": "2"}'],
        )

    def test_show_rows_unknown_source_or_batch_is_rejected(self) -> None:
        self.register_orders()
        self.import_rows("order_id,total\n1,10\n")
        self.digest_before = self.db_digest()
        for arguments in [("missing", "1"), ("orders", "9")]:
            with self.subTest(arguments=arguments):
                result = self.invoke("show-rows", *arguments)
                self.assertEqual(result.returncode, 1)
                self.assertTrue(result.stderr.startswith("Error:"))
                self.assertEqual(len(result.stderr.strip().splitlines()), 1)
                self.assertEqual(result.stdout, "")
                self.assertEqual(self.db_digest(), self.digest_before)

    def test_show_rows_rejects_invalid_arguments(self) -> None:
        self.register_orders()
        self.import_rows("order_id,total\n1,10\n")
        self.digest_before = self.db_digest()
        for arguments in [("orders", "0"), ("orders", "-1"), ("TRUE", "1")]:
            with self.subTest(arguments=arguments):
                result = self.invoke("show-rows", *arguments)
                self.assertEqual(result.returncode, 1)
                self.assertTrue(result.stderr.startswith("Error:"))
                self.assertEqual(result.stdout, "")
                self.assertEqual(self.db_digest(), self.digest_before)

    def test_find_dupes_merges_rows_across_batches(self) -> None:
        self.register_orders()
        self.import_rows("order_id,total\n1,10\n2,20\n")
        self.import_rows("order_id,total\n3,10\n4,30\n")
        self.digest_before = self.db_digest()
        result = self.invoke("find-dupes", "orders", "amount")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.splitlines(), ["10\t2\t1,2"])
        self.assert_readonly(result)

    def test_find_dupes_excludes_revoked_batch_rows(self) -> None:
        self.register_orders()
        self.import_rows("order_id,total\n1,10\n2,20\n")
        self.import_rows("order_id,total\n3,10\n4,20\n")
        self.assertEqual(self.invoke("revoke-batch", "orders", "1").returncode, 0)
        result = self.invoke("find-dupes", "orders", "amount")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout, "")

    def test_find_dupes_no_duplicates_is_empty(self) -> None:
        self.register_orders()
        self.import_rows("order_id,total\n1,10\n2,20\n")
        result = self.invoke("find-dupes", "orders", "amount")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout, "")

    def test_find_dupes_groups_sorted_by_value(self) -> None:
        self.register_orders()
        self.import_rows("order_id,total\n1,20\n2,10\n")
        self.import_rows("order_id,total\n3,20\n4,10\n")
        result = self.invoke("find-dupes", "orders", "amount")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(
            result.stdout.splitlines(), ["10\t2\t1,2", "20\t2\t1,2"]
        )

    def test_find_dupes_unknown_target_column_is_rejected(self) -> None:
        self.register_orders()
        self.import_rows("order_id,total\n1,10\n")
        self.digest_before = self.db_digest()
        result = self.invoke("find-dupes", "orders", "missing")
        self.assertEqual(result.returncode, 1)
        self.assertTrue(result.stderr.startswith("Error:"))
        self.assertEqual(len(result.stderr.strip().splitlines()), 1)
        self.assertEqual(result.stdout, "")
        self.assertEqual(self.db_digest(), self.digest_before)

    def test_find_dupes_rejects_invalid_arguments(self) -> None:
        self.register_orders()
        self.import_rows("order_id,total\n1,10\n")
        self.digest_before = self.db_digest()
        for arguments in [("missing", "amount"), ("TRUE", "amount"), ("orders", "  ")]:
            with self.subTest(arguments=arguments):
                result = self.invoke("find-dupes", *arguments)
                self.assertEqual(result.returncode, 1)
                self.assertTrue(result.stderr.startswith("Error:"))
                self.assertEqual(result.stdout, "")
                self.assertEqual(self.db_digest(), self.digest_before)


if __name__ == "__main__":
    unittest.main()
