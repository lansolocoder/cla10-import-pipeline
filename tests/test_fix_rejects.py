"""Checks for the fix-rejects / apply-fixed review subcommands."""

from pathlib import Path
import os
import sqlite3
import subprocess
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]


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

    def batch_row(self, batch_no: int = 1) -> tuple:
        conn = sqlite3.connect(self.db_path)
        try:
            return conn.execute(
                "SELECT total_rows, imported_rows, duplicate_rows, rejected_rows,"
                " incremental_rows, created_by FROM import_batches"
                " WHERE source_name = 'orders' AND batch_no = ?",
                (batch_no,),
            ).fetchone()
        finally:
            conn.close()

    def record_signatures(self) -> set[str]:
        conn = sqlite3.connect(self.db_path)
        try:
            return {
                row[0]
                for row in conn.execute(
                    "SELECT signature FROM imported_records WHERE source_name = 'orders'"
                )
            }
        finally:
            conn.close()

    def import_with_rejects(self) -> None:
        csv_path = self.write_csv(
            "orders.csv", "order_id,total\nA1,10\nA2,\nB2\n"
        )
        self.register_orders(csv_path)
        result = self.invoke("import", "orders")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "Result: import orders 1 3 1 0 2 1")

    def test_fix_rejects_exports_suspicious_rows_and_appends_fix_column(self) -> None:
        self.import_with_rejects()
        result = self.invoke("fix-rejects", "orders", "1")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "Result: fix-rejects orders 1 2")
        self.assertEqual(result.stderr, "")

        # 原拒绝文件重写：表头加可疑行、原样按出现顺序。
        self.assertEqual(
            (self.work / "rejected_orders_1.csv").read_text(encoding="utf-8"),
            "order_id,total\nA2,\nB2\n",
        )
        # 复核文件：原表头追加 fix_result，可疑行原样在前、结果留空。
        self.assertEqual(
            (self.work / "rejected_orders_1_fix.csv").read_text(encoding="utf-8"),
            "order_id,total,fix_result\nA2,,\nB2,\n",
        )
        # 批次计数与签名不变。
        self.assertEqual(self.batch_row(), (3, 1, 0, 2, 1, "import"))
        self.assertEqual(len(self.record_signatures()), 1)

    def test_fix_rejects_reimport_batch_is_rejected(self) -> None:
        self.import_with_rejects()
        self.assertEqual(self.invoke("reimport", "orders", "1").returncode, 0)
        result = self.invoke("fix-rejects", "orders", "1")
        self.assertEqual(result.returncode, 1)
        self.assertIn("reimport", result.stderr)
        self.assertNotIn("Result:", result.stdout)
        # reimport 批次的拒绝文件仍在，但不应生成复核文件。
        self.assertFalse((self.work / "rejected_orders_1_fix.csv").exists())
        self.assertEqual(self.batch_row()[5], "reimport")

    def test_fix_rejects_zero_suspicious_rows_errors_without_files(self) -> None:
        csv_path = self.write_csv("orders.csv", "order_id,total\nA1,10\n")
        self.register_orders(csv_path)
        self.assertEqual(self.invoke("import", "orders").returncode, 0)
        result = self.invoke("fix-rejects", "orders", "1")
        self.assertEqual(result.returncode, 1)
        self.assertNotEqual(result.stderr, "")
        self.assertFalse((self.work / "rejected_orders_1.csv").exists())
        self.assertFalse((self.work / "rejected_orders_1_fix.csv").exists())
        self.assertEqual(self.batch_row()[:5], (1, 1, 0, 0, 1))

    def test_fix_rejects_missing_batch_and_unknown_source_are_errors(self) -> None:
        self.import_with_rejects()
        result = self.invoke("fix-rejects", "orders", "9")
        self.assertEqual(result.returncode, 1)
        self.assertIn("9", result.stderr)
        result = self.invoke("fix-rejects", "missing", "1")
        self.assertEqual(result.returncode, 1)
        self.assertIn("missing", result.stderr)
        self.assertFalse((self.work / "rejected_orders_9_fix.csv").exists())

    def test_fix_rejects_invalid_batch_number_is_usage_error(self) -> None:
        for bad in ("0", "-1", "01", "1.0", "abc"):
            with self.subTest(bad=bad):
                result = self.invoke("fix-rejects", "orders", bad)
                self.assertEqual(result.returncode, 2)
                self.assertEqual(result.stdout, "")

    def test_fix_rejects_uses_fix_result_n_when_header_collides(self) -> None:
        csv_path = self.write_csv(
            "orders.csv", "order_id,total,fix_result\nA1,10,x\nA2,,y\n"
        )
        self.register_orders(csv_path)
        self.assertEqual(
            self.invoke("add-mapping", "orders", "fix_result", "note").returncode, 0
        )
        self.assertEqual(self.invoke("import", "orders").returncode, 0)
        self.assertEqual(self.invoke("fix-rejects", "orders", "1").returncode, 0)
        self.assertEqual(
            (self.work / "rejected_orders_1_fix.csv").read_text(encoding="utf-8"),
            "order_id,total,fix_result,fix_result_1\nA2,,y,\n",
        )


class ApplyFixedTests(unittest.TestCase):
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

    def write_fix_file(self, content: str) -> None:
        (self.work / "rejected_orders_1_fix.csv").write_text(
            content, encoding="utf-8"
        )

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

    def batch_row(self) -> tuple:
        conn = sqlite3.connect(self.db_path)
        try:
            return conn.execute(
                "SELECT total_rows, imported_rows, duplicate_rows, rejected_rows,"
                " incremental_rows FROM import_batches"
                " WHERE source_name = 'orders' AND batch_no = 1"
            ).fetchone()
        finally:
            conn.close()

    def prepare(self, csv_content: str) -> None:
        csv_path = (self.work / "orders.csv")
        csv_path.write_text(csv_content, encoding="utf-8")
        self.register_orders(str(csv_path))
        self.assertEqual(self.invoke("import", "orders").returncode, 0)
        self.assertEqual(self.invoke("fix-rejects", "orders", "1").returncode, 0)

    def test_apply_fixed_imports_ok_rows_and_writes_rejected_result_file(self) -> None:
        self.prepare("order_id,total\nA1,10\nA2,\nB2\n")
        # 人工修复 A2（补 amount 并标 ok）；B2 保持列数不符并标 rejected。
        self.write_fix_file("order_id,total,fix_result\nA2,20,ok\nB2,,rejected\n")

        result = self.invoke("apply-fixed", "orders", "1")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "Result: apply-fixed orders 1 1 0 1")
        self.assertEqual(result.stderr, "")

        # 总行数不变；成功/增量各 +1，校验失败 -2。
        self.assertEqual(self.batch_row(), (3, 2, 0, 0, 2))
        # 修复文件被改写为 fix 结果文件（含 rejected 行），原拒绝文件始终保留。
        self.assertTrue((self.work / "rejected_orders_1_fix.csv").is_file())
        self.assertEqual(
            (self.work / "rejected_orders_1.csv").read_text(encoding="utf-8"),
            "order_id,total\nA2,\nB2\n",
        )
        self.assertFalse(list(self.work.glob("*.tmp")))

        conn = sqlite3.connect(self.db_path)
        try:
            batches = conn.execute(
                "SELECT batch_no FROM imported_records WHERE source_name = 'orders'"
            ).fetchall()
        finally:
            conn.close()
        # 新签名归属原批次号 1。
        self.assertEqual({row[0] for row in batches}, {1})

    def test_apply_fixed_rejected_rows_go_to_result_file_in_order(self) -> None:
        self.prepare("order_id,total\nA1,10\nA2,\nB2\n")
        self.write_fix_file("order_id,total,fix_result\nA2,20,ok\nB2,,rejected\n")
        self.assertEqual(self.invoke("apply-fixed", "orders", "1").returncode, 0)
        # rejected 行写回同名 fix 文件，表头与原 CSV 一致、按修复文件顺序。
        result_path = self.work / "rejected_orders_1_fix.csv"
        self.assertTrue(result_path.is_file())
        self.assertEqual(
            result_path.read_text(encoding="utf-8"), "order_id,total\nB2,\n"
        )

    def test_apply_fixed_all_ok_deletes_repair_file_and_makes_no_result_file(self) -> None:
        self.prepare("order_id,total\nA1,10\nA2,\n")
        self.write_fix_file("order_id,total,fix_result\nA2,20,ok\n")
        result = self.invoke("apply-fixed", "orders", "1")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "Result: apply-fixed orders 1 1 0 0")
        self.assertEqual(self.batch_row(), (2, 2, 0, 0, 2))
        self.assertFalse((self.work / "rejected_orders_1_fix.csv").exists())
        self.assertTrue((self.work / "rejected_orders_1.csv").is_file())

    def test_apply_fixed_duplicate_signature_is_discarded(self) -> None:
        # A1 已导入；把可疑行修成与 A1 相同的记录 -> 判重丢弃。
        self.prepare("order_id,total\nA1,10\nA2,\n")
        before = self.batch_row()
        sigs_before = self.record_signatures()
        self.write_fix_file("order_id,total,fix_result\nA1,10,ok\n")
        result = self.invoke("apply-fixed", "orders", "1")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "Result: apply-fixed orders 1 0 1 0")
        # 总行数不变；重复 +1，校验失败 -1；成功/增量不变。
        self.assertEqual(
            self.batch_row(),
            (before[0], before[1], before[2] + 1, before[3] - 1, before[4]),
        )
        self.assertEqual(self.record_signatures(), sigs_before)
        self.assertFalse((self.work / "rejected_orders_1_fix.csv").exists())

    def record_signatures(self) -> set[str]:
        conn = sqlite3.connect(self.db_path)
        try:
            return {
                row[0]
                for row in conn.execute(
                    "SELECT signature FROM imported_records WHERE source_name = 'orders'"
                )
            }
        finally:
            conn.close()

    def test_apply_fixed_invalid_verdict_changes_nothing(self) -> None:
        self.prepare("order_id,total\nA1,10\nA2,\n")
        before = self.batch_row()
        sigs_before = self.record_signatures()
        for content in (
            "order_id,total,fix_result\nA2,20,OK\n",
            "order_id,total,fix_result\nA2,20,\n",
            "order_id,total,fix_result\nA2,20,maybe\n",
        ):
            with self.subTest(content=content):
                self.write_fix_file(content)
                result = self.invoke("apply-fixed", "orders", "1")
                self.assertEqual(result.returncode, 1)
                self.assertNotEqual(result.stderr, "")
                self.assertEqual(self.batch_row(), before)
                self.assertEqual(self.record_signatures(), sigs_before)
                # 修复文件保留、原拒绝文件保留。
                self.assertTrue(
                    (self.work / "rejected_orders_1_fix.csv").is_file()
                )

    def test_apply_fixed_ok_row_failing_revalidation_changes_nothing(self) -> None:
        self.prepare("order_id,total\nA1,10\nA2,\nB2\n")
        before = self.batch_row()
        sigs_before = self.record_signatures()
        # A2 标 ok 但必需字段仍为空。
        self.write_fix_file("order_id,total,fix_result\nA2,,ok\nB2,,rejected\n")
        result = self.invoke("apply-fixed", "orders", "1")
        self.assertEqual(result.returncode, 1)
        self.assertNotIn("Result:", result.stdout)
        self.assertEqual(self.batch_row(), before)
        self.assertEqual(self.record_signatures(), sigs_before)
        # 列数仍与表头不一致的 ok 行同样整体失败。
        self.write_fix_file("order_id,total,fix_result\nB2,ok\n")
        result = self.invoke("apply-fixed", "orders", "1")
        self.assertEqual(result.returncode, 1)
        self.assertEqual(self.batch_row(), before)
        self.assertEqual(self.record_signatures(), sigs_before)
        self.assertFalse(list(self.work.glob("*.tmp")))

    def test_apply_fixed_without_fix_rejects_is_error(self) -> None:
        csv_path = (self.work / "orders.csv")
        csv_path.write_text("order_id,total\nA1,10\nA2,\n", encoding="utf-8")
        self.register_orders(str(csv_path))
        self.assertEqual(self.invoke("import", "orders").returncode, 0)
        result = self.invoke("apply-fixed", "orders", "1")
        self.assertEqual(result.returncode, 1)
        self.assertIn("fix-rejects", result.stderr)
        self.assertEqual(self.batch_row(), (2, 1, 0, 1, 1))

    def test_apply_fixed_missing_repair_file_is_error_without_changes(self) -> None:
        self.prepare("order_id,total\nA1,10\nA2,\n")
        before = self.batch_row()
        (self.work / "rejected_orders_1_fix.csv").unlink()
        result = self.invoke("apply-fixed", "orders", "1")
        self.assertEqual(result.returncode, 1)
        self.assertIn("修复文件不存在", result.stderr)
        self.assertEqual(self.batch_row(), before)

    def test_apply_fixed_unknown_source_and_missing_batch_are_errors(self) -> None:
        self.prepare("order_id,total\nA1,10\nA2,\n")
        result = self.invoke("apply-fixed", "missing", "1")
        self.assertEqual(result.returncode, 1)
        self.assertIn("missing", result.stderr)
        result = self.invoke("apply-fixed", "orders", "9")
        self.assertEqual(result.returncode, 1)
        self.assertIn("9", result.stderr)

    def test_apply_fixed_invalid_batch_number_is_usage_error(self) -> None:
        for bad in ("0", "-1", "01", "1.0", "abc"):
            with self.subTest(bad=bad):
                result = self.invoke("apply-fixed", "orders", bad)
                self.assertEqual(result.returncode, 2)
                self.assertEqual(result.stdout, "")

    def test_apply_fixed_after_reimport_is_rejected_despite_stale_file(self) -> None:
        self.prepare("order_id,total\nA1,10\nA2,\n")
        # 重跑批次：批次转为 reimport 创建，旧复核标记应失效。
        result = self.invoke("reimport", "orders", "1")
        self.assertEqual(result.returncode, 0, result.stderr)
        after_reimport = self.batch_row()
        # 磁盘上的复核文件即使仍在，也不能再应用。
        self.write_fix_file("order_id,total,fix_result\nA2,20,ok\n")
        result = self.invoke("apply-fixed", "orders", "1")
        self.assertEqual(result.returncode, 1)
        self.assertIn("reimport", result.stderr)
        self.assertEqual(self.batch_row(), after_reimport)

    def test_apply_fixed_then_fix_rejects_reports_no_suspicious_rows(self) -> None:
        self.prepare("order_id,total\nA1,10\nA2,\n")
        self.write_fix_file("order_id,total,fix_result\nA2,20,ok\n")
        self.assertEqual(self.invoke("apply-fixed", "orders", "1").returncode, 0)
        result = self.invoke("fix-rejects", "orders", "1")
        self.assertEqual(result.returncode, 1)
        self.assertFalse((self.work / "rejected_orders_1_fix.csv").exists())

    def test_apply_fixed_with_fix_result_n_column_collision(self) -> None:
        csv_path = (self.work / "orders.csv")
        csv_path.write_text(
            "order_id,total,fix_result\nA1,10,x\nA2,,y\n", encoding="utf-8"
        )
        self.register_orders(str(csv_path))
        self.assertEqual(
            self.invoke("add-mapping", "orders", "fix_result", "note").returncode, 0
        )
        self.assertEqual(self.invoke("import", "orders").returncode, 0)
        self.assertEqual(self.invoke("fix-rejects", "orders", "1").returncode, 0)
        self.write_fix_file(
            "order_id,total,fix_result,fix_result_1\nA2,,y,rejected\n"
        )
        result = self.invoke("apply-fixed", "orders", "1")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "Result: apply-fixed orders 1 0 0 1")
        # fix 结果文件表头与原 CSV 一致（含业务 fix_result 列，无 fix_result_1）。
        self.assertEqual(
            (self.work / "rejected_orders_1_fix.csv").read_text(encoding="utf-8"),
            "order_id,total,fix_result\nA2,,y\n",
        )
        self.assertEqual(self.batch_row(), (2, 1, 0, 0, 1))


if __name__ == "__main__":
    unittest.main()
