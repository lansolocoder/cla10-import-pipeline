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


class BatchOperationTests(unittest.TestCase):
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

    def record_rows(self) -> list[tuple]:
        conn = sqlite3.connect(self.db_path)
        try:
            return conn.execute(
                "SELECT signature, batch_no FROM imported_records"
                " WHERE source_name = 'orders' ORDER BY rowid"
            ).fetchall()
        finally:
            conn.close()

    def test_batches_lists_rows_ascending_tab_separated(self) -> None:
        csv_path = self.write_csv("orders.csv", "order_id,total\nA1,10\nA2,20\n")
        self.register_orders(csv_path)
        self.assertEqual(self.invoke("import", "orders").returncode, 0)
        Path(csv_path).write_text(
            "order_id,total\nA1,10\nA2,20\n", encoding="utf-8"
        )
        self.assertEqual(self.invoke("import", "orders").returncode, 0)

        result = self.invoke("batches", "orders")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(
            result.stdout.splitlines(),
            [
                "1\t2\t2\t0\t0\t2",
                "2\t2\t0\t2\t0\t0",
                "Result: batches orders",
            ],
        )
        self.assertEqual(result.stderr, "")

    def test_batches_unknown_source_is_error(self) -> None:
        result = self.invoke("batches", "missing")
        self.assertEqual(result.returncode, 1)
        self.assertIn("missing", result.stderr)
        self.assertEqual(result.stdout, "")

    def test_batches_empty_outputs_only_result_line(self) -> None:
        csv_path = self.write_csv("orders.csv", "order_id,total\nA1,10\n")
        self.register_orders(csv_path)
        result = self.invoke("batches", "orders")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "Result: batches orders")

    def test_reimport_replaces_batch_atomically_with_same_number(self) -> None:
        csv_path = self.write_csv("orders.csv", "order_id,total\nA1,10\nA2,20\n")
        self.register_orders(csv_path)
        self.assertEqual(self.invoke("import", "orders").returncode, 0)
        self.assertEqual(self.invoke("import", "orders").returncode, 0)
        # 重跑前 A1/A2 签名均归属批次 1（批次 2 全部判重，无签名）。
        self.assertEqual(len(self.record_rows()), 2)

        # 扩展 CSV：重跑批次 1 时 A1/A2 与其自身签名判重，A3 为增量。
        Path(csv_path).write_text(
            "order_id,total\nA1,10\nA2,20\nA3,30\n", encoding="utf-8"
        )
        result = self.invoke("reimport", "orders", "1")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "Result: reimport orders 1 3 1 2 0 1")
        self.assertEqual(result.stderr, "")

        listing = self.invoke("batches", "orders").stdout.splitlines()
        self.assertEqual(
            listing,
            [
                "1\t3\t1\t2\t0\t1",
                "2\t2\t0\t2\t0\t0",
                "Result: batches orders",
            ],
        )
        # 批次 1 的签名被整体替换：A1/A2（判重）删除，仅 A3 归属批次 1。
        sigs = self.record_rows()
        self.assertEqual(len(sigs), 1)
        self.assertEqual(sigs[0][1], 1)

    def test_reimport_missing_batch_is_error_without_changes(self) -> None:
        csv_path = self.write_csv("orders.csv", "order_id,total\nA1,10\n")
        self.register_orders(csv_path)
        self.assertEqual(self.invoke("import", "orders").returncode, 0)
        result = self.invoke("reimport", "orders", "9")
        self.assertEqual(result.returncode, 1)
        self.assertIn("9", result.stderr)
        self.assertNotIn("Result:", result.stdout)
        self.assertEqual(
            self.invoke("batches", "orders").stdout.splitlines(),
            ["1\t1\t1\t0\t0\t1", "Result: batches orders"],
        )

    def test_reimport_precheck_failure_changes_nothing(self) -> None:
        csv_path = self.write_csv("orders.csv", "order_id,total\nA1,10\n")
        self.register_orders(csv_path)
        self.assertEqual(self.invoke("import", "orders").returncode, 0)
        before_sigs = self.record_rows()
        Path(csv_path).unlink()
        result = self.invoke("reimport", "orders", "1")
        self.assertEqual(result.returncode, 1)
        self.assertEqual(self.record_rows(), before_sigs)
        self.assertEqual(
            self.invoke("batches", "orders").stdout.splitlines(),
            ["1\t1\t1\t0\t0\t1", "Result: batches orders"],
        )

    def test_reimport_unknown_source_is_error(self) -> None:
        result = self.invoke("reimport", "missing", "1")
        self.assertEqual(result.returncode, 1)
        self.assertIn("missing", result.stderr)

    def test_rollback_deletes_only_that_batch_signatures(self) -> None:
        csv_path = self.write_csv("orders.csv", "order_id,total\nA1,10\nA2,20\n")
        self.register_orders(csv_path)
        self.assertEqual(self.invoke("import", "orders").returncode, 0)
        Path(csv_path).write_text(
            "order_id,total\nA3,30\n", encoding="utf-8"
        )
        self.assertEqual(self.invoke("import", "orders").returncode, 0)

        result = self.invoke("rollback", "orders", "1")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "Result: rollback orders 1 2")
        self.assertEqual(result.stderr, "")
        self.assertFalse((self.work / "rejected_orders_1.csv").exists())

        self.assertEqual(
            self.invoke("batches", "orders").stdout.splitlines(),
            ["2\t1\t1\t0\t0\t1", "Result: batches orders"],
        )
        # 仅批次 2 的签名保留；批次 1 的 A1/A2 可再次导入。
        remaining = {row[1] for row in self.record_rows()}
        self.assertEqual(remaining, {2})
        Path(csv_path).write_text(
            "order_id,total\nA1,10\nA3,30\n", encoding="utf-8"
        )
        result = self.invoke("import", "orders")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "Result: import orders 3 2 1 1 0 1")

    def test_rollback_missing_batch_is_error_without_changes(self) -> None:
        csv_path = self.write_csv("orders.csv", "order_id,total\nA1,10\n")
        self.register_orders(csv_path)
        self.assertEqual(self.invoke("import", "orders").returncode, 0)
        result = self.invoke("rollback", "orders", "9")
        self.assertEqual(result.returncode, 1)
        self.assertIn("9", result.stderr)
        self.assertNotIn("Result:", result.stdout)
        self.assertEqual(
            self.invoke("batches", "orders").stdout.splitlines(),
            ["1\t1\t1\t0\t0\t1", "Result: batches orders"],
        )

    def test_rollback_unknown_source_is_error(self) -> None:
        result = self.invoke("rollback", "missing", "1")
        self.assertEqual(result.returncode, 1)
        self.assertIn("missing", result.stderr)

    def test_invalid_batch_numbers_are_usage_errors(self) -> None:
        for bad in ("0", "-1", "01", "007", "1.0", "abc", "1a"):
            for command in ("reimport", "rollback", "fix-rejects", "apply-fixed"):
                with self.subTest(command=command, bad=bad):
                    result = self.invoke(command, "orders", bad)
                    self.assertEqual(result.returncode, 2)
                    self.assertNotEqual(result.stderr, "")
                    self.assertEqual(result.stdout, "")


class FixRejectsTests(unittest.TestCase):
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

    def import_with_suspicious_rows(self) -> str:
        csv_path = self.write_csv(
            "orders.csv", "order_id,total\nA1,10\nA2,\nB3\n"
        )
        self.register_orders(csv_path)
        result = self.invoke("import", "orders")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "Result: import orders 1 3 1 0 2 1")
        return csv_path

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

    def test_fix_rejects_writes_reject_and_fix_files(self) -> None:
        self.import_with_suspicious_rows()
        result = self.invoke("fix-rejects", "orders", "1")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "Result: fix-rejects orders 1 2")
        self.assertEqual(result.stderr, "")

        rejected = (self.work / "rejected_orders_1.csv").read_text(encoding="utf-8")
        self.assertEqual(rejected.splitlines(), ["order_id,total", "A2,", "B3"])
        fix = (self.work / "rejected_orders_1_fix.csv").read_text(encoding="utf-8")
        self.assertEqual(
            fix.splitlines(),
            ["order_id,total,fix_result", "A2,,", "B3,"],
        )
        # 批次计数与签名不变。
        self.assertEqual(self.batch_rows(), [("orders", 1, 3, 1, 0, 2, 1)])

    def test_fix_rejects_reimport_batch_is_error_without_files(self) -> None:
        csv_path = self.import_with_suspicious_rows()
        self.assertEqual(self.invoke("reimport", "orders", "1").returncode, 0)
        (self.work / "rejected_orders_1.csv").unlink()
        result = self.invoke("fix-rejects", "orders", "1")
        self.assertEqual(result.returncode, 1)
        self.assertNotEqual(result.stderr, "")
        self.assertNotIn("Result:", result.stdout)
        self.assertEqual(list(self.work.glob("rejected_*.csv")), [])
        # 计数保持 reimport 后的状态不变。
        self.assertEqual(self.batch_rows(), [("orders", 1, 3, 0, 1, 2, 0)])

    def test_fix_rejects_zero_suspicious_rows_is_error(self) -> None:
        csv_path = self.write_csv("orders.csv", "order_id,total\nA1,10\n")
        self.register_orders(csv_path)
        self.assertEqual(self.invoke("import", "orders").returncode, 0)
        result = self.invoke("fix-rejects", "orders", "1")
        self.assertEqual(result.returncode, 1)
        self.assertNotEqual(result.stderr, "")
        self.assertEqual(list(self.work.glob("rejected_*.csv")), [])

    def test_fix_rejects_missing_batch_is_error(self) -> None:
        csv_path = self.write_csv("orders.csv", "order_id,total\nA1,10\n")
        self.register_orders(csv_path)
        result = self.invoke("fix-rejects", "orders", "9")
        self.assertEqual(result.returncode, 1)
        self.assertIn("9", result.stderr)
        self.assertNotIn("Result:", result.stdout)

    def test_fix_rejects_conflicting_fix_result_column_uses_suffix(self) -> None:
        csv_path = self.write_csv(
            "notes.csv", "note_id,fix_result\nN1,x\nN2\n"
        )
        self.assertEqual(
            self.invoke("add-source", "notes", csv_path, "id").returncode, 0
        )
        self.assertEqual(
            self.invoke("add-mapping", "notes", "note_id", "id").returncode, 0
        )
        self.assertEqual(
            self.invoke("add-mapping", "notes", "fix_result", "note").returncode, 0
        )
        self.assertEqual(self.invoke("import", "notes").returncode, 0)
        result = self.invoke("fix-rejects", "notes", "1")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "Result: fix-rejects notes 1 1")
        fix = (self.work / "rejected_notes_1_fix.csv").read_text(encoding="utf-8")
        self.assertEqual(
            fix.splitlines(), ["note_id,fix_result,fix_result_1", "N2,"]
        )

    def test_apply_fixed_imports_ok_rows_and_updates_counts(self) -> None:
        csv_path = self.import_with_suspicious_rows()
        self.assertEqual(self.invoke("fix-rejects", "orders", "1").returncode, 0)
        (self.work / "rejected_orders_1_fix.csv").write_text(
            "order_id,total,fix_result\nA2,20,ok\nB3,30,ok\n", encoding="utf-8"
        )
        result = self.invoke("apply-fixed", "orders", "1")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "Result: apply-fixed orders 1 2 0 0")
        self.assertEqual(result.stderr, "")
        # 总行数不变，成功/增量各加 2，校验失败减 2；批次号不变。
        self.assertEqual(self.batch_rows(), [("orders", 1, 3, 3, 0, 0, 3)])
        # 修复文件被删除且无 rejected 行时不生成 fix 结果文件；原拒绝文件保留。
        self.assertFalse((self.work / "rejected_orders_1_fix.csv").exists())
        self.assertTrue((self.work / "rejected_orders_1.csv").is_file())
        # 修复导入的签名参与后续判重。
        Path(csv_path).write_text(
            "order_id,total\nA1,10\nA2,20\nB3,30\n", encoding="utf-8"
        )
        result = self.invoke("import", "orders")
        self.assertEqual(result.stdout.strip(), "Result: import orders 2 3 0 3 0 0")

    def test_apply_fixed_counts_duplicates_and_writes_rejected_rows(self) -> None:
        self.import_with_suspicious_rows()
        self.assertEqual(self.invoke("fix-rejects", "orders", "1").returncode, 0)
        # A1,10 与批次 1 已导入签名重复；B3 标记 rejected。
        (self.work / "rejected_orders_1_fix.csv").write_text(
            "order_id,total,fix_result\nA1,10,ok\nB3,30,rejected\n",
            encoding="utf-8",
        )
        result = self.invoke("apply-fixed", "orders", "1")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "Result: apply-fixed orders 1 0 1 1")
        self.assertEqual(self.batch_rows(), [("orders", 1, 3, 1, 1, 0, 1)])
        fix = (self.work / "rejected_orders_1_fix.csv").read_text(encoding="utf-8")
        self.assertEqual(fix.splitlines(), ["order_id,total", "B3,30"])

    def test_apply_fixed_invalid_decision_is_error_without_changes(self) -> None:
        self.import_with_suspicious_rows()
        self.assertEqual(self.invoke("fix-rejects", "orders", "1").returncode, 0)
        before = self.batch_rows()
        (self.work / "rejected_orders_1_fix.csv").write_text(
            "order_id,total,fix_result\nA2,20,OK\nB3,30,ok\n", encoding="utf-8"
        )
        result = self.invoke("apply-fixed", "orders", "1")
        self.assertEqual(result.returncode, 1)
        self.assertNotEqual(result.stderr, "")
        self.assertNotIn("Result:", result.stdout)
        self.assertEqual(self.batch_rows(), before)
        self.assertTrue((self.work / "rejected_orders_1_fix.csv").is_file())

    def test_apply_fixed_still_invalid_ok_row_is_error_without_changes(self) -> None:
        self.import_with_suspicious_rows()
        self.assertEqual(self.invoke("fix-rejects", "orders", "1").returncode, 0)
        before = self.batch_rows()
        (self.work / "rejected_orders_1_fix.csv").write_text(
            "order_id,total,fix_result\nA2,,ok\nB3,30,ok\n", encoding="utf-8"
        )
        result = self.invoke("apply-fixed", "orders", "1")
        self.assertEqual(result.returncode, 1)
        self.assertNotEqual(result.stderr, "")
        self.assertEqual(self.batch_rows(), before)
        self.assertTrue((self.work / "rejected_orders_1_fix.csv").is_file())

    def test_apply_fixed_missing_fix_file_is_error(self) -> None:
        self.import_with_suspicious_rows()
        before = self.batch_rows()
        result = self.invoke("apply-fixed", "orders", "1")
        self.assertEqual(result.returncode, 1)
        self.assertNotEqual(result.stderr, "")
        self.assertNotIn("Result:", result.stdout)
        self.assertEqual(self.batch_rows(), before)

    def test_apply_fixed_missing_batch_is_error(self) -> None:
        csv_path = self.write_csv("orders.csv", "order_id,total\nA1,10\n")
        self.register_orders(csv_path)
        result = self.invoke("apply-fixed", "orders", "9")
        self.assertEqual(result.returncode, 1)
        self.assertIn("9", result.stderr)
        self.assertNotIn("Result:", result.stdout)


if __name__ == "__main__":
    unittest.main()
