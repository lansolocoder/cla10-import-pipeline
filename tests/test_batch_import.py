"""Checks for the batch-import multi-file subcommand."""

from pathlib import Path
import os
import sqlite3
import subprocess
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]


class BatchImportCommandTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.db_path = Path(self.tmp.name) / "import_ledger.db"
        self.csv_path = Path(self.tmp.name) / "orders.csv"
        self.part1 = Path(self.tmp.name) / "part1.csv"
        self.part2 = Path(self.tmp.name) / "part2.csv"
        self.part3 = Path(self.tmp.name) / "part3.csv"

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

    def write(self, path: Path, text: str) -> None:
        path.write_text(text, encoding="utf-8")

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

    def assert_one_error_line(self, result: subprocess.CompletedProcess[str]) -> None:
        self.assertEqual(result.returncode, 1)
        self.assertTrue(result.stderr.startswith("Error:"))
        self.assertEqual(len(result.stderr.strip().splitlines()), 1)
        self.assertEqual(result.stdout, "")

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

    def test_merges_multiple_files_into_one_ok_batch(self) -> None:
        self.register_orders()
        self.write(self.part1, "order_id,total\n1,10\n2,20\n")
        self.write(self.part2, "order_id,total\n3,30\n")
        result = self.invoke(
            "batch-import", "orders", str(self.part1), str(self.part2)
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stderr, "")
        self.assertEqual(result.stdout.strip(), "Result: batch-import orders 3")
        self.assertEqual(self.batches(), [(1, "ok", 3, 0)])
        self.assertEqual(self.imported_row_count(), 3)

        show = self.invoke("show-rows", "orders", "1")
        self.assertEqual(show.returncode, 0, show.stderr)
        self.assertEqual(
            show.stdout.splitlines(),
            [
                '1\t{"amount": "10", "id": "1"}',
                '2\t{"amount": "20", "id": "2"}',
                '3\t{"amount": "30", "id": "3"}',
            ],
        )

    def test_row_numbers_continue_across_files_in_argument_order(self) -> None:
        self.register_orders()
        self.write(self.part1, "order_id,total\n3,30\n")
        self.write(self.part2, "order_id,total\n1,10\n2,20\n")
        result = self.invoke(
            "batch-import", "orders", str(self.part2), str(self.part1)
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        show = self.invoke("show-rows", "orders", "1")
        self.assertEqual(
            show.stdout.splitlines(),
            [
                '1\t{"amount": "10", "id": "1"}',
                '2\t{"amount": "20", "id": "2"}',
                '3\t{"amount": "30", "id": "3"}',
            ],
        )

    def test_headers_may_differ_in_order_but_not_in_set(self) -> None:
        self.register_orders()
        self.write(self.part1, "order_id,total\n1,10\n")
        self.write(self.part2, "total,order_id\n20,2\n")
        result = self.invoke(
            "batch-import", "orders", str(self.part1), str(self.part2)
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        show = self.invoke("show-rows", "orders", "1")
        self.assertEqual(
            show.stdout.splitlines(),
            [
                '1\t{"amount": "10", "id": "1"}',
                '2\t{"amount": "20", "id": "2"}',
            ],
        )

    def test_duplicate_paths_are_treated_as_ordinary_files(self) -> None:
        self.register_orders()
        self.write(self.part1, "order_id,total\n1,10\n")
        result = self.invoke(
            "batch-import", "orders", str(self.part1), str(self.part1)
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "Result: batch-import orders 2")
        self.assertEqual(self.batches(), [(1, "ok", 2, 0)])
        self.assertEqual(self.imported_row_count(), 2)

    def test_fewer_than_two_files_is_rejected_without_batch(self) -> None:
        self.register_orders()
        self.write(self.part1, "order_id,total\n1,10\n")
        result = self.invoke("batch-import", "orders", str(self.part1))
        self.assert_one_error_line(result)
        self.assertEqual(self.batches(), [])
        self.assertEqual(self.imported_row_count(), 0)

    def test_missing_file_leaves_no_batch_record(self) -> None:
        self.register_orders()
        self.write(self.part1, "order_id,total\n1,10\n")
        missing = Path(self.tmp.name) / "nope.csv"
        result = self.invoke(
            "batch-import", "orders", str(self.part1), str(missing)
        )
        self.assert_one_error_line(result)
        self.assertEqual(self.batches(), [])
        self.assertEqual(self.imported_row_count(), 0)

    def test_directory_path_leaves_no_batch_record(self) -> None:
        self.register_orders()
        self.write(self.part1, "order_id,total\n1,10\n")
        result = self.invoke(
            "batch-import", "orders", str(self.part1), self.tmp.name
        )
        self.assert_one_error_line(result)
        self.assertEqual(self.batches(), [])

    @unittest.skipUnless(
        hasattr(os, "geteuid") and os.geteuid() != 0, "需要非 root 才能制造权限拒绝"
    )
    def test_unreadable_file_leaves_no_batch_record(self) -> None:
        self.register_orders()
        self.write(self.part1, "order_id,total\n1,10\n")
        self.write(self.part2, "order_id,total\n2,20\n")
        os.chmod(self.part2, 0o000)
        try:
            result = self.invoke(
                "batch-import", "orders", str(self.part1), str(self.part2)
            )
            self.assert_one_error_line(result)
            self.assertEqual(self.batches(), [])
        finally:
            os.chmod(self.part2, 0o644)

    def test_empty_file_without_header_leaves_no_batch_record(self) -> None:
        self.register_orders()
        self.write(self.part1, "order_id,total\n1,10\n")
        self.write(self.part2, "")
        result = self.invoke(
            "batch-import", "orders", str(self.part1), str(self.part2)
        )
        self.assert_one_error_line(result)
        self.assertEqual(self.batches(), [])

    def test_blank_header_column_leaves_no_batch_record(self) -> None:
        self.register_orders()
        self.write(self.part1, "order_id,total\n1,10\n")
        self.write(self.part2, "order_id,\n2,20\n")
        result = self.invoke(
            "batch-import", "orders", str(self.part1), str(self.part2)
        )
        self.assert_one_error_line(result)
        self.assertEqual(self.batches(), [])

    def test_header_set_mismatch_leaves_no_batch_record(self) -> None:
        self.register_orders()
        self.write(self.part1, "order_id,total\n1,10\n")
        self.write(self.part2, "order_id\n2\n")
        result = self.invoke(
            "batch-import", "orders", str(self.part1), str(self.part2)
        )
        self.assert_one_error_line(result)
        self.assertEqual(self.batches(), [])
        self.assertEqual(self.imported_row_count(), 0)

    def test_unmapped_column_rejects_all_files(self) -> None:
        self.register_orders()
        self.write(self.part1, "order_id,total,extra\n1,10,a\n")
        self.write(self.part2, "order_id,total,extra\n2,20,x\n3,30,y\n")
        result = self.invoke(
            "batch-import", "orders", str(self.part1), str(self.part2)
        )
        self.assert_one_error_line(result)
        self.assertEqual(self.batches(), [(1, "rejected", 0, 3)])
        self.assertEqual(self.imported_row_count(), 0)

    def test_missing_required_target_rejects_all_files(self) -> None:
        result = self.invoke(
            "add-source", "orders", str(self.csv_path), "id", "amount"
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(
            self.invoke("add-mapping", "orders", "order_id", "id").returncode, 0
        )
        self.write(self.part1, "order_id\n1\n")
        self.write(self.part2, "order_id\n2\n3\n")
        result = self.invoke(
            "batch-import", "orders", str(self.part1), str(self.part2)
        )
        self.assert_one_error_line(result)
        self.assertEqual(self.batches(), [(1, "rejected", 0, 3)])
        self.assertEqual(self.imported_row_count(), 0)

    def test_blank_value_in_later_file_rejects_with_total_quarantine(self) -> None:
        self.register_orders()
        self.write(self.part1, "order_id,total\n1,10\n2,20\n")
        self.write(self.part2, "order_id,total\n3,  \n")
        result = self.invoke(
            "batch-import", "orders", str(self.part1), str(self.part2)
        )
        self.assert_one_error_line(result)
        self.assertEqual(self.batches(), [(1, "rejected", 0, 3)])
        self.assertEqual(self.imported_row_count(), 0)

    def test_wrong_column_count_rejects_batch(self) -> None:
        self.register_orders()
        self.write(self.part1, "order_id,total\n1,10\n")
        self.write(self.part2, "order_id,total\n2,20,30\n")
        result = self.invoke(
            "batch-import", "orders", str(self.part1), str(self.part2)
        )
        self.assert_one_error_line(result)
        self.assertEqual(self.batches(), [(1, "rejected", 0, 2)])
        self.assertEqual(self.imported_row_count(), 0)

    def test_file_level_failure_after_prior_batch_keeps_db_state(self) -> None:
        self.register_orders()
        self.write(self.csv_path, "order_id,total\n9,90\n")
        self.assertEqual(self.invoke("run-import", "orders").returncode, 0)
        self.write(self.part1, "order_id,total\n1,10\n")
        result = self.invoke(
            "batch-import", "orders", str(self.part1), str(self.part2)
        )
        self.assert_one_error_line(result)
        self.assertEqual(self.batches(), [(1, "ok", 1, 0)])
        self.assertEqual(self.imported_row_count(), 1)

    def test_batch_number_continues_per_source_sequence(self) -> None:
        self.register_orders()
        self.write(self.csv_path, "order_id,total\n9,90\n")
        self.assertEqual(self.invoke("run-import", "orders").returncode, 0)

        self.write(self.part1, "order_id,total\n1,10\n")
        self.write(self.part2, "order_id,total\n2,  \n")
        rejected = self.invoke(
            "batch-import", "orders", str(self.part1), str(self.part2)
        )
        self.assertEqual(rejected.returncode, 1)

        self.write(self.part2, "order_id,total\n2,20\n3,30\n")
        ok = self.invoke(
            "batch-import", "orders", str(self.part1), str(self.part2)
        )
        self.assertEqual(ok.returncode, 0, ok.stderr)
        self.assertEqual(ok.stdout.strip(), "Result: batch-import orders 3")
        self.assertEqual(
            self.batches(),
            [(1, "ok", 1, 0), (2, "rejected", 0, 2), (3, "ok", 3, 0)],
        )
        self.assertEqual(self.imported_row_count(), 4)

    def test_unknown_source_is_rejected_without_batch(self) -> None:
        self.write(self.part1, "order_id,total\n1,10\n")
        self.write(self.part2, "order_id,total\n2,20\n")
        result = self.invoke(
            "batch-import", "missing", str(self.part1), str(self.part2)
        )
        self.assert_one_error_line(result)

    def test_find_dupes_merges_within_and_across_batches(self) -> None:
        self.register_orders()
        self.write(self.part1, "order_id,total\n1,10\n2,10\n")
        self.write(self.part2, "order_id,total\n3,10\n")
        result = self.invoke(
            "batch-import", "orders", str(self.part1), str(self.part2)
        )
        self.assertEqual(result.returncode, 0, result.stderr)

        self.write(self.csv_path, "order_id,total\n4,10\n5,50\n")
        self.assertEqual(self.invoke("run-import", "orders").returncode, 0)

        dupes = self.invoke("find-dupes", "orders", "amount")
        self.assertEqual(dupes.returncode, 0, dupes.stderr)
        self.assertEqual(dupes.stdout.splitlines(), ["10\t4\t1,2"])

    def test_revoke_batch_imported_batch(self) -> None:
        self.register_orders()
        self.write(self.part1, "order_id,total\n1,10\n")
        self.write(self.part2, "order_id,total\n2,20\n3,30\n")
        result = self.invoke(
            "batch-import", "orders", str(self.part1), str(self.part2)
        )
        self.assertEqual(result.returncode, 0, result.stderr)

        revoked = self.invoke("revoke-batch", "orders", "1")
        self.assertEqual(revoked.returncode, 0, revoked.stderr)
        self.assertEqual(revoked.stdout.strip(), "Result: revoke-batch orders 1")
        self.assertEqual(self.batches(), [(1, "revoked", 3, 0)])
        self.assertEqual(self.imported_row_count(), 0)
        show = self.invoke("show-rows", "orders", "1")
        self.assertEqual(show.returncode, 0)
        self.assertEqual(show.stdout, "")


if __name__ == "__main__":
    unittest.main()
