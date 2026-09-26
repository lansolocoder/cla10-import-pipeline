"""Checks for the source administration subcommands."""

from pathlib import Path
import os
import sqlite3
import subprocess
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]


class SourceAdminTests(unittest.TestCase):
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

    def ledger_counts(self) -> dict[str, int]:
        conn = sqlite3.connect(self.db_path)
        try:
            return {
                table: conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                for table in (
                    "sources",
                    "source_fields",
                    "field_mappings",
                    "import_batches",
                    "imported_records",
                )
            }
        finally:
            conn.close()

    def test_update_source_changes_csv_path(self) -> None:
        old_path = self.write_csv("old.csv", "order_id,total\nA1,10\n")
        new_path = self.write_csv("new.csv", "order_id,total\nB1,20\nB2,30\n")
        self.register_orders(old_path)

        result = self.invoke("update-source", "orders", new_path)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(
            result.stdout.strip(), f"Result: update-source orders {new_path}"
        )
        self.assertEqual(
            self.invoke("list-sources").stdout.splitlines(),
            [f"orders\t{new_path}\tid,amount"],
        )
        # 此后 import 读取新路径。
        result = self.invoke("import", "orders")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "Result: import orders 1 2 2 0 0 2")

    def test_update_source_unknown_source_is_error_without_changes(self) -> None:
        csv_path = self.write_csv("orders.csv", "order_id,total\nA1,10\n")
        self.register_orders(csv_path)
        before = self.ledger_counts()
        result = self.invoke("update-source", "missing", "other.csv")
        self.assertEqual(result.returncode, 1)
        self.assertIn("missing", result.stderr)
        self.assertNotIn("Result:", result.stdout)
        self.assertEqual(self.ledger_counts(), before)
        self.assertEqual(
            self.invoke("list-sources").stdout.splitlines(),
            [f"orders\t{csv_path}\tid,amount"],
        )

    def test_update_source_rejects_invalid_path(self) -> None:
        csv_path = self.write_csv("orders.csv", "order_id,total\nA1,10\n")
        self.register_orders(csv_path)
        for bad in ("", "   ", "TRUE"):
            with self.subTest(bad=bad):
                result = self.invoke("update-source", "orders", bad)
                self.assertEqual(result.returncode, 1)
                self.assertNotEqual(result.stderr, "")
                self.assertNotIn("Result:", result.stdout)
        self.assertEqual(
            self.invoke("list-sources").stdout.splitlines(),
            [f"orders\t{csv_path}\tid,amount"],
        )

    def test_remove_mapping_deletes_one_mapping(self) -> None:
        csv_path = self.write_csv("orders.csv", "order_id,total\nA1,10\n")
        self.register_orders(csv_path)
        result = self.invoke("remove-mapping", "orders", "order_id")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(
            result.stdout.strip(), "Result: remove-mapping orders order_id"
        )
        self.assertEqual(
            self.invoke("list-mappings", "orders").stdout.splitlines(),
            ["total\tamount"],
        )

    def test_remove_mapping_unknown_column_is_error_without_changes(self) -> None:
        csv_path = self.write_csv("orders.csv", "order_id,total\nA1,10\n")
        self.register_orders(csv_path)
        result = self.invoke("remove-mapping", "orders", "nope")
        self.assertEqual(result.returncode, 1)
        self.assertIn("nope", result.stderr)
        self.assertNotIn("Result:", result.stdout)
        self.assertEqual(
            self.invoke("list-mappings", "orders").stdout.splitlines(),
            ["order_id\tid", "total\tamount"],
        )

    def test_remove_mapping_unknown_source_is_error(self) -> None:
        result = self.invoke("remove-mapping", "missing", "order_id")
        self.assertEqual(result.returncode, 1)
        self.assertIn("missing", result.stderr)
        self.assertNotIn("Result:", result.stdout)

    def test_retire_source_removes_config_but_keeps_history(self) -> None:
        csv_path = self.write_csv("orders.csv", "order_id,total\nA1,10\nA2,20\n")
        self.register_orders(csv_path)
        self.assertEqual(self.invoke("import", "orders").returncode, 0)
        rejected = self.work / "rejected_keep.csv"
        rejected.write_text("order_id,total\nB1,\n", encoding="utf-8")

        result = self.invoke("retire-source", "orders")
        self.assertEqual(result.returncode, 0, result.stderr)
        # 2 个必需字段 + 2 条字段映射。
        self.assertEqual(result.stdout.strip(), "Result: retire-source orders 4")

        self.assertEqual(self.invoke("list-sources").stdout, "")
        # 批次记录与导入签名全部保留，仅来源配置与映射被删除。
        counts = self.ledger_counts()
        self.assertEqual(counts["sources"], 0)
        self.assertEqual(counts["source_fields"], 0)
        self.assertEqual(counts["field_mappings"], 0)
        self.assertEqual(counts["import_batches"], 1)
        self.assertEqual(counts["imported_records"], 2)
        # 拒绝文件不受影响。
        self.assertTrue(rejected.is_file())
        # 退役后来源未注册，batches 按既有规则报错。
        result = self.invoke("batches", "orders")
        self.assertEqual(result.returncode, 1)
        self.assertIn("orders", result.stderr)

    def test_retire_then_reregister_starts_fresh(self) -> None:
        csv_path = self.write_csv("orders.csv", "order_id,total\nA1,10\nA2,20\n")
        self.register_orders(csv_path)
        self.assertEqual(self.invoke("import", "orders").returncode, 0)
        self.assertEqual(self.invoke("retire-source", "orders").returncode, 0)

        # 同名来源可重新注册，沿用 add-source 全部校验。
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
        # 旧签名不再参与判重：全部行计为增量新数据，批次号从 1 重新开始。
        result = self.invoke("import", "orders")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "Result: import orders 1 2 2 0 0 2")
        self.assertEqual(
            self.invoke("batches", "orders").stdout.splitlines(),
            ["1\t2\t2\t0\t0\t2", "Result: batches orders"],
        )

    def test_retire_unknown_source_is_error_without_changes(self) -> None:
        csv_path = self.write_csv("orders.csv", "order_id,total\nA1,10\n")
        self.register_orders(csv_path)
        before = self.ledger_counts()
        result = self.invoke("retire-source", "missing")
        self.assertEqual(result.returncode, 1)
        self.assertIn("missing", result.stderr)
        self.assertNotIn("Result:", result.stdout)
        self.assertEqual(self.ledger_counts(), before)

    def test_retire_twice_uses_distinct_tombstones(self) -> None:
        csv_path = self.write_csv("orders.csv", "order_id,total\nA1,10\n")
        for _ in range(2):
            self.register_orders(csv_path)
            self.assertEqual(self.invoke("import", "orders").returncode, 0)
            result = self.invoke("retire-source", "orders")
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(result.stdout.strip(), "Result: retire-source orders 4")
        counts = self.ledger_counts()
        self.assertEqual(counts["import_batches"], 2)
        self.assertEqual(counts["imported_records"], 2)

    def test_missing_arguments_are_usage_errors(self) -> None:
        for arguments in (
            ("update-source", "orders"),
            ("remove-mapping", "orders"),
            ("retire-source",),
        ):
            with self.subTest(arguments=arguments):
                result = self.invoke(*arguments)
                self.assertEqual(result.returncode, 2)
                self.assertNotEqual(result.stderr, "")


if __name__ == "__main__":
    unittest.main()
