"""Checks for the batch-import subcommand (multi-file single batch)."""

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
        self.registered_csv = Path(self.tmp.name) / "orders.csv"
        self.part1 = Path(self.tmp.name) / "part1.csv"
        self.part2 = Path(self.tmp.name) / "part2.csv"

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

    def register_orders(self) -> None:
        result = self.invoke(
            "add-source", "orders", str(self.registered_csv), "id", "amount"
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

    def imported_rows(self) -> list[tuple[int, int, str]]:
        conn = sqlite3.connect(self.db_path)
        try:
            return conn.execute(
                "SELECT batch_no, row_number, data FROM imported_rows"
                " ORDER BY batch_no, row_number"
            ).fetchall()
        finally:
            conn.close()

    def imported_row_count(self) -> int:
        return len(self.imported_rows())

    # ---- 成功路径 ----------------------------------------------------------

    def test_multiple_files_merge_into_single_ok_batch(self) -> None:
        self.register_orders()
        self.part1.write_text("order_id,total\n1,10\n2,20\n", encoding="utf-8")
        self.part2.write_text("order_id,total\n3,30\n4,40\n5,50\n", encoding="utf-8")

        result = self.invoke(
            "batch-import", "orders", str(self.part1), str(self.part2)
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stderr, "")
        self.assertEqual(result.stdout.strip(), "Result: batch-import orders 5")
        self.assertEqual(self.batches(), [(1, "ok", 5, 0)])
        self.assertEqual(self.imported_row_count(), 5)

    def test_row_numbers_are_continuous_across_files(self) -> None:
        self.register_orders()
        self.part1.write_text("order_id,total\n1,10\n2,20\n", encoding="utf-8")
        self.part2.write_text("order_id,total\n3,30\n", encoding="utf-8")
        result = self.invoke(
            "batch-import", "orders", str(self.part1), str(self.part2)
        )
        self.assertEqual(result.returncode, 0, result.stderr)

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

    def test_file_argument_order_determines_row_order(self) -> None:
        self.register_orders()
        self.part1.write_text("order_id,total\n1,10\n", encoding="utf-8")
        self.part2.write_text("order_id,total\n2,20\n", encoding="utf-8")
        result = self.invoke(
            "batch-import", "orders", str(self.part2), str(self.part1)
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        show = self.invoke("show-rows", "orders", "1")
        self.assertEqual(
            show.stdout.splitlines(),
            ['1\t{"amount": "20", "id": "2"}', '2\t{"amount": "10", "id": "1"}'],
        )

    def test_headers_same_set_different_order_map_per_file(self) -> None:
        self.register_orders()
        self.part1.write_text("order_id,total\n1,10\n", encoding="utf-8")
        self.part2.write_text("total,order_id\n20,2\n", encoding="utf-8")
        result = self.invoke(
            "batch-import", "orders", str(self.part1), str(self.part2)
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        show = self.invoke("show-rows", "orders", "1")
        self.assertEqual(
            show.stdout.splitlines(),
            ['1\t{"amount": "10", "id": "1"}', '2\t{"amount": "20", "id": "2"}'],
        )

    def test_duplicate_paths_are_treated_as_plain_files(self) -> None:
        self.register_orders()
        self.part1.write_text("order_id,total\n1,10\n", encoding="utf-8")
        result = self.invoke(
            "batch-import", "orders", str(self.part1), str(self.part1)
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "Result: batch-import orders 2")
        self.assertEqual(self.batches(), [(1, "ok", 2, 0)])
        show = self.invoke("show-rows", "orders", "1")
        self.assertEqual(
            show.stdout.splitlines(),
            ['1\t{"amount": "10", "id": "1"}', '2\t{"amount": "10", "id": "1"}'],
        )

    def test_duplicate_target_values_land_without_merging(self) -> None:
        self.register_orders()
        self.part1.write_text("order_id,total\n1,10\n2,10\n", encoding="utf-8")
        self.part2.write_text("order_id,total\n3,10\n", encoding="utf-8")
        result = self.invoke(
            "batch-import", "orders", str(self.part1), str(self.part2)
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.imported_row_count(), 3)
        dupes = self.invoke("find-dupes", "orders", "amount")
        self.assertEqual(dupes.returncode, 0, dupes.stderr)
        self.assertEqual(dupes.stdout.splitlines(), ["10\t3\t1"])

    def test_find_dupes_merges_same_batch_and_cross_batch(self) -> None:
        self.register_orders()
        self.registered_csv.write_text(
            "order_id,total\n9,10\n", encoding="utf-8"
        )
        self.assertEqual(self.invoke("run-import", "orders").returncode, 0)
        self.part1.write_text("order_id,total\n1,10\n2,20\n", encoding="utf-8")
        self.part2.write_text("order_id,total\n3,10\n", encoding="utf-8")
        result = self.invoke(
            "batch-import", "orders", str(self.part1), str(self.part2)
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(
            self.batches(), [(1, "ok", 1, 0), (2, "ok", 3, 0)]
        )
        dupes = self.invoke("find-dupes", "orders", "amount")
        self.assertEqual(dupes.returncode, 0, dupes.stderr)
        self.assertEqual(dupes.stdout.splitlines(), ["10\t3\t1,2"])

    def test_batch_number_continues_after_existing_batches(self) -> None:
        self.register_orders()
        self.registered_csv.write_text("order_id,total\n1,10\n", encoding="utf-8")
        self.assertEqual(self.invoke("run-import", "orders").returncode, 0)
        self.part1.write_text("order_id,total\n2,20\n", encoding="utf-8")
        self.part2.write_text("order_id,total\n3,30\n", encoding="utf-8")
        result = self.invoke(
            "batch-import", "orders", str(self.part1), str(self.part2)
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(
            self.batches(), [(1, "ok", 1, 0), (2, "ok", 2, 0)]
        )
        show = self.invoke("show-rows", "orders", "2")
        self.assertEqual(
            show.stdout.splitlines(),
            ['1\t{"amount": "20", "id": "2"}', '2\t{"amount": "30", "id": "3"}'],
        )

    def test_revoke_batch_imported_batch_deletes_all_rows(self) -> None:
        self.register_orders()
        self.part1.write_text("order_id,total\n1,10\n2,20\n", encoding="utf-8")
        self.part2.write_text("order_id,total\n3,30\n", encoding="utf-8")
        self.assertEqual(
            self.invoke(
                "batch-import", "orders", str(self.part1), str(self.part2)
            ).returncode,
            0,
        )
        result = self.invoke("revoke-batch", "orders", "1")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "Result: revoke-batch orders 1")
        self.assertEqual(self.batches(), [(1, "revoked", 3, 0)])
        self.assertEqual(self.imported_row_count(), 0)
        self.assertEqual(
            self.invoke("show-rows", "orders", "1").stdout.splitlines(), []
        )

    # ---- 校验失败：登记 rejected 批次 --------------------------------------

    def test_unmapped_column_rejects_whole_batch(self) -> None:
        self.register_orders()
        self.part1.write_text("order_id,total,extra\n1,10,a\n", encoding="utf-8")
        self.part2.write_text("order_id,total,extra\n2,20,b\n", encoding="utf-8")
        result = self.invoke(
            "batch-import", "orders", str(self.part1), str(self.part2)
        )
        self.assertEqual(result.returncode, 1)
        self.assertTrue(result.stderr.startswith("Error:"))
        self.assertEqual(len(result.stderr.strip().splitlines()), 1)
        self.assertEqual(result.stdout, "")
        self.assertEqual(self.batches(), [(1, "rejected", 0, 2)])
        self.assertEqual(self.imported_row_count(), 0)

    def test_missing_required_target_column_rejects_all(self) -> None:
        self.register_orders()
        self.part1.write_text("order_id\n1\n", encoding="utf-8")
        self.part2.write_text("order_id\n2\n", encoding="utf-8")
        result = self.invoke(
            "batch-import", "orders", str(self.part1), str(self.part2)
        )
        self.assertEqual(result.returncode, 1)
        self.assertTrue(result.stderr.startswith("Error:"))
        self.assertEqual(result.stdout, "")
        self.assertEqual(self.batches(), [(1, "rejected", 0, 2)])
        self.assertEqual(self.imported_row_count(), 0)

    def test_blank_value_quarantines_total_row_count_of_all_files(self) -> None:
        self.register_orders()
        self.part1.write_text("order_id,total\n1,10\n2,20\n", encoding="utf-8")
        self.part2.write_text("order_id,total\n3, \n4,40\n", encoding="utf-8")
        result = self.invoke(
            "batch-import", "orders", str(self.part1), str(self.part2)
        )
        self.assertEqual(result.returncode, 1)
        self.assertTrue(result.stderr.startswith("Error:"))
        self.assertEqual(result.stdout, "")
        self.assertEqual(self.batches(), [(1, "rejected", 0, 4)])
        self.assertEqual(self.imported_row_count(), 0)

    def test_wrong_column_count_rejects_all(self) -> None:
        self.register_orders()
        self.part1.write_text("order_id,total\n1,10\n", encoding="utf-8")
        self.part2.write_text("order_id,total\n2\n", encoding="utf-8")
        result = self.invoke(
            "batch-import", "orders", str(self.part1), str(self.part2)
        )
        self.assertEqual(result.returncode, 1)
        self.assertTrue(result.stderr.startswith("Error:"))
        self.assertEqual(self.batches(), [(1, "rejected", 0, 2)])
        self.assertEqual(self.imported_row_count(), 0)

    # ---- 预检失败：不产生批次记录、数据库保持原状 ---------------------------

    def assert_preflight_failure(self, *paths: str) -> None:
        result = self.invoke("batch-import", "orders", *paths)
        self.assertEqual(result.returncode, 1)
        self.assertTrue(result.stderr.startswith("Error:"))
        self.assertEqual(len(result.stderr.strip().splitlines()), 1)
        self.assertEqual(result.stdout, "")
        self.assertEqual(self.batches(), [])
        self.assertEqual(self.imported_row_count(), 0)

    def test_fewer_than_two_files_leaves_no_batch(self) -> None:
        self.register_orders()
        self.part1.write_text("order_id,total\n1,10\n", encoding="utf-8")
        self.assert_preflight_failure(str(self.part1))

    def test_missing_file_leaves_no_batch(self) -> None:
        self.register_orders()
        self.part1.write_text("order_id,total\n1,10\n", encoding="utf-8")
        missing = Path(self.tmp.name) / "nope.csv"
        self.assert_preflight_failure(str(self.part1), str(missing))

    def test_directory_path_leaves_no_batch(self) -> None:
        self.register_orders()
        self.part1.write_text("order_id,total\n1,10\n", encoding="utf-8")
        self.assert_preflight_failure(str(self.part1), self.tmp.name)

    def test_empty_file_missing_header_leaves_no_batch(self) -> None:
        self.register_orders()
        self.part1.write_text("order_id,total\n1,10\n", encoding="utf-8")
        self.part2.write_text("", encoding="utf-8")
        self.assert_preflight_failure(str(self.part1), str(self.part2))

    def test_blank_header_column_name_leaves_no_batch(self) -> None:
        self.register_orders()
        self.part1.write_text("order_id,total\n1,10\n", encoding="utf-8")
        self.part2.write_text("order_id,\n2,20\n", encoding="utf-8")
        self.assert_preflight_failure(str(self.part1), str(self.part2))

    def test_inconsistent_header_sets_leave_no_batch(self) -> None:
        self.register_orders()
        self.part1.write_text("order_id,total\n1,10\n", encoding="utf-8")
        self.part2.write_text("order_id,total,extra\n2,20,x\n", encoding="utf-8")
        self.assert_preflight_failure(str(self.part1), str(self.part2))

    def test_preflight_failure_keeps_existing_state(self) -> None:
        self.register_orders()
        self.registered_csv.write_text("order_id,total\n1,10\n", encoding="utf-8")
        self.assertEqual(self.invoke("run-import", "orders").returncode, 0)
        self.part1.write_text("order_id,total\n2,20\n", encoding="utf-8")
        missing = Path(self.tmp.name) / "nope.csv"

        result = self.invoke(
            "batch-import", "orders", str(self.part1), str(missing)
        )
        self.assertEqual(result.returncode, 1)
        self.assertTrue(result.stderr.startswith("Error:"))
        self.assertEqual(result.stdout, "")
        self.assertEqual(self.batches(), [(1, "ok", 1, 0)])
        self.assertEqual(self.imported_row_count(), 1)

    # ---- 来源校验 -----------------------------------------------------------

    def test_unknown_source_is_rejected_without_batch(self) -> None:
        self.part1.write_text("a,b\n1,2\n", encoding="utf-8")
        self.part2.write_text("a,b\n3,4\n", encoding="utf-8")
        result = self.invoke(
            "batch-import", "missing", str(self.part1), str(self.part2)
        )
        self.assertEqual(result.returncode, 1)
        self.assertTrue(result.stderr.startswith("Error:"))
        self.assertEqual(result.stdout, "")
        # 不留任何来源、批次或数据行
        conn = sqlite3.connect(self.db_path)
        try:
            self.assertEqual(
                conn.execute("SELECT COUNT(*) FROM sources").fetchone()[0], 0
            )
            self.assertEqual(
                conn.execute("SELECT COUNT(*) FROM batches").fetchone()[0], 0
            )
            self.assertEqual(
                conn.execute("SELECT COUNT(*) FROM imported_rows").fetchone()[0], 0
            )
        finally:
            conn.close()

    def test_reserved_literal_source_name_is_rejected(self) -> None:
        self.register_orders()
        self.part1.write_text("order_id,total\n1,10\n", encoding="utf-8")
        self.part2.write_text("order_id,total\n2,20\n", encoding="utf-8")
        result = self.invoke(
            "batch-import", "TRUE", str(self.part1), str(self.part2)
        )
        self.assertEqual(result.returncode, 1)
        self.assertTrue(result.stderr.startswith("Error:"))
        self.assertEqual(result.stdout, "")
        self.assertEqual(self.batches(), [])


if __name__ == "__main__":
    unittest.main()
