"""Checks for the batches / reimport / rollback subcommands."""

from pathlib import Path
import os
import sqlite3
import subprocess
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]


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

    def signatures(self) -> set[str]:
        conn = sqlite3.connect(self.db_path)
        try:
            return {
                row[0]
                for row in conn.execute(
                    "SELECT signature FROM imported_records"
                    " WHERE source_name = 'orders'"
                )
            }
        finally:
            conn.close()


class BatchesCommandTests(BatchOperationTests):
    def test_no_batches_outputs_nothing(self) -> None:
        csv_path = self.write_csv("orders.csv", "order_id,total\nA1,10\n")
        self.register_orders(csv_path)
        result = self.invoke("batches", "orders")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout, "")
        self.assertEqual(result.stderr, "")

    def test_unknown_source_fails(self) -> None:
        result = self.invoke("batches", "missing")
        self.assertEqual(result.returncode, 1)
        self.assertIn("missing", result.stderr)
        self.assertEqual(result.stdout, "")

    def test_lists_batches_in_order(self) -> None:
        csv_path = self.write_csv("orders.csv", "order_id,total\nA1,10\nA2,\n")
        self.register_orders(csv_path)
        self.assertEqual(self.invoke("import", "orders").returncode, 0)
        Path(csv_path).write_text(
            "order_id,total\nA1,10\nA3,30\n", encoding="utf-8"
        )
        self.assertEqual(self.invoke("import", "orders").returncode, 0)

        result = self.invoke("batches", "orders")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(
            result.stdout.splitlines(),
            ["1\t2\t1\t0\t1\t1", "2\t2\t1\t1\t0\t1", "Result: batches orders"],
        )


class ReimportCommandTests(BatchOperationTests):
    def test_reimport_replaces_batch_record(self) -> None:
        csv_path = self.write_csv("orders.csv", "order_id,total\nA1,10\nA2,\n")
        self.register_orders(csv_path)
        self.assertEqual(self.invoke("import", "orders").returncode, 0)
        self.assertEqual(self.invoke("import", "orders").returncode, 0)

        Path(csv_path).write_text(
            "order_id,total\nA1,10\nA2,20\nA3,30\n", encoding="utf-8"
        )
        result = self.invoke("reimport", "orders", "1")
        self.assertEqual(result.returncode, 0, result.stderr)
        # A1 已被批次 1 自身导入记为重复；A2 补齐必需字段、A3 为增量新数据
        self.assertEqual(
            result.stdout.strip(), "Result: reimport orders 1 3 2 1 0 2"
        )
        self.assertEqual(
            self.batch_rows(),
            [("orders", 1, 3, 2, 1, 0, 2), ("orders", 2, 2, 0, 1, 1, 0)],
        )
        # 批次 1 的旧签名（A1）被整体替换掉，仅剩本次新增的 A2、A3
        self.assertEqual(len(self.signatures()), 2)

    def test_reimport_writes_reject_file_with_same_batch_no(self) -> None:
        csv_path = self.write_csv("orders.csv", "order_id,total\nA1,10\n")
        self.register_orders(csv_path)
        self.assertEqual(self.invoke("import", "orders").returncode, 0)
        Path(csv_path).write_text(
            "order_id,total\nA1,10\nA2,\n", encoding="utf-8"
        )
        result = self.invoke("reimport", "orders", "1")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(
            result.stdout.strip(), "Result: reimport orders 1 2 0 1 1 0"
        )
        rejected = self.work / "rejected_orders_1.csv"
        self.assertTrue(rejected.is_file())
        self.assertEqual(
            rejected.read_text(encoding="utf-8"), "order_id,total\nA2,\n"
        )

    def test_reimport_missing_batch_fails_without_side_effects(self) -> None:
        csv_path = self.write_csv("orders.csv", "order_id,total\nA1,10\n")
        self.register_orders(csv_path)
        self.assertEqual(self.invoke("import", "orders").returncode, 0)
        before_batches = self.batch_rows()
        before_signatures = self.signatures()

        result = self.invoke("reimport", "orders", "9")
        self.assertEqual(result.returncode, 1)
        self.assertNotEqual(result.stderr, "")
        self.assertEqual(result.stdout, "")
        self.assertEqual(self.batch_rows(), before_batches)
        self.assertEqual(self.signatures(), before_signatures)
        self.assertEqual(list(self.work.glob("rejected_*.csv")), [])

    def test_reimport_invalid_batch_no_is_usage_error(self) -> None:
        csv_path = self.write_csv("orders.csv", "order_id,total\nA1,10\n")
        self.register_orders(csv_path)
        self.assertEqual(self.invoke("import", "orders").returncode, 0)
        for bad in ["0", "-1", "1.0", "abc", ""]:
            with self.subTest(batch_no=bad):
                result = self.invoke("reimport", "orders", bad)
                self.assertEqual(result.returncode, 2)
                self.assertEqual(result.stdout, "")


class RollbackCommandTests(BatchOperationTests):
    def test_rollback_removes_only_own_batch(self) -> None:
        csv_path = self.write_csv("orders.csv", "order_id,total\nA1,10\nA2,20\n")
        self.register_orders(csv_path)
        self.assertEqual(self.invoke("import", "orders").returncode, 0)
        Path(csv_path).write_text(
            "order_id,total\nA1,10\nA3,30\n", encoding="utf-8"
        )
        self.assertEqual(self.invoke("import", "orders").returncode, 0)
        conn = sqlite3.connect(self.db_path)
        try:
            kept = {
                row[0]
                for row in conn.execute(
                    "SELECT signature FROM imported_records WHERE batch_no = 1"
                )
            }
        finally:
            conn.close()

        result = self.invoke("rollback", "orders", "2")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "Result: rollback orders 2 2")
        self.assertEqual(self.batch_rows(), [("orders", 1, 2, 2, 0, 0, 2)])
        self.assertEqual(self.signatures(), kept)

    def test_rollback_allows_reimport_of_same_rows(self) -> None:
        csv_path = self.write_csv("orders.csv", "order_id,total\nA1,10\n")
        self.register_orders(csv_path)
        self.assertEqual(self.invoke("import", "orders").returncode, 0)
        self.assertEqual(self.invoke("rollback", "orders", "1").returncode, 0)
        result = self.invoke("import", "orders")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "Result: import orders 1 1 1 0 0 1")

    def test_rollback_missing_batch_fails_without_side_effects(self) -> None:
        csv_path = self.write_csv("orders.csv", "order_id,total\nA1,10\n")
        self.register_orders(csv_path)
        self.assertEqual(self.invoke("import", "orders").returncode, 0)
        before_batches = self.batch_rows()
        before_signatures = self.signatures()

        result = self.invoke("rollback", "orders", "7")
        self.assertEqual(result.returncode, 1)
        self.assertNotEqual(result.stderr, "")
        self.assertEqual(result.stdout, "")
        self.assertEqual(self.batch_rows(), before_batches)
        self.assertEqual(self.signatures(), before_signatures)

    def test_rollback_invalid_batch_no_is_usage_error(self) -> None:
        csv_path = self.write_csv("orders.csv", "order_id,total\nA1,10\n")
        self.register_orders(csv_path)
        for bad in ["0", "-2", "x"]:
            with self.subTest(batch_no=bad):
                result = self.invoke("rollback", "orders", bad)
                self.assertEqual(result.returncode, 2)
                self.assertEqual(result.stdout, "")


if __name__ == "__main__":
    unittest.main()
