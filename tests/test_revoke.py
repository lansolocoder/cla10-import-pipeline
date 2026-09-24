"""Checks for the revoke-batch subcommand."""

from pathlib import Path
import os
import sqlite3
import subprocess
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]


class RevokeCommandTests(unittest.TestCase):
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

    def import_ok_batch(self, text: str = "order_id,total\n1,10\n2,20\n") -> None:
        self.write_csv(text)
        result = self.invoke("run-import", "orders")
        self.assertEqual(result.returncode, 0, result.stderr)

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

    def assert_rejected(self, result: subprocess.CompletedProcess[str]) -> None:
        self.assertEqual(result.returncode, 1)
        self.assertTrue(result.stderr.startswith("Error:"), result.stderr)
        self.assertEqual(len(result.stderr.strip().splitlines()), 1)
        self.assertEqual(result.stdout, "")

    def test_revoke_ok_batch_removes_rows_and_marks_revoked(self) -> None:
        self.register_orders()
        self.import_ok_batch()

        result = self.invoke("revoke-batch", "orders", "1")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "Result: revoke-batch orders 1")
        self.assertEqual(result.stderr, "")
        self.assertEqual(self.batches(), [(1, "revoked", 2, 0)])
        self.assertEqual(self.imported_row_count(), 0)

        shown = self.invoke("show-batch", "orders", "1")
        self.assertEqual(shown.returncode, 0, shown.stderr)
        self.assertEqual(shown.stdout.splitlines(), ["1\trevoked\t2\t0"])

    def test_revoke_leaves_other_batches_untouched(self) -> None:
        self.register_orders()
        self.import_ok_batch("order_id,total\n1,10\n")
        self.import_ok_batch("order_id,total\n2,20\n3,30\n")

        result = self.invoke("revoke-batch", "orders", "1")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(
            self.batches(), [(1, "revoked", 1, 0), (2, "ok", 2, 0)]
        )
        self.assertEqual(self.imported_row_count(), 2)

    def test_second_revoke_is_rejected_without_changes(self) -> None:
        self.register_orders()
        self.import_ok_batch()
        self.assertEqual(self.invoke("revoke-batch", "orders", "1").returncode, 0)

        result = self.invoke("revoke-batch", "orders", "1")
        self.assert_rejected(result)
        self.assertEqual(self.batches(), [(1, "revoked", 2, 0)])
        self.assertEqual(self.imported_row_count(), 0)

    def test_revoke_rejected_batch_is_rejected_without_changes(self) -> None:
        self.register_orders()
        self.write_csv("order_id,total\n1,\n")
        self.assertEqual(self.invoke("run-import", "orders").returncode, 1)

        result = self.invoke("revoke-batch", "orders", "1")
        self.assert_rejected(result)
        self.assertEqual(self.batches(), [(1, "rejected", 0, 1)])
        self.assertEqual(self.imported_row_count(), 0)

    def test_revoke_unknown_source_or_batch_is_rejected(self) -> None:
        self.register_orders()
        self.import_ok_batch()

        result = self.invoke("revoke-batch", "missing", "1")
        self.assert_rejected(result)
        result = self.invoke("revoke-batch", "orders", "9")
        self.assert_rejected(result)
        self.assertEqual(self.batches(), [(1, "ok", 2, 0)])
        self.assertEqual(self.imported_row_count(), 2)

    def test_revoke_non_positive_batch_no_is_rejected(self) -> None:
        self.register_orders()
        self.import_ok_batch()

        for batch_no in ["0", "-1"]:
            with self.subTest(batch_no=batch_no):
                result = self.invoke("revoke-batch", "orders", batch_no)
                self.assert_rejected(result)
        self.assertEqual(self.batches(), [(1, "ok", 2, 0)])
        self.assertEqual(self.imported_row_count(), 2)

    def test_revoke_reserved_literal_source_is_rejected(self) -> None:
        self.register_orders()
        self.import_ok_batch()

        result = self.invoke("revoke-batch", "TRUE", "1")
        self.assert_rejected(result)
        self.assertEqual(self.batches(), [(1, "ok", 2, 0)])
        self.assertEqual(self.imported_row_count(), 2)


if __name__ == "__main__":
    unittest.main()
