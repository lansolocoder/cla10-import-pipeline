"""Checks for the source-management subcommands: update-source / remove-mapping / retire-source."""

from pathlib import Path
import os
import sqlite3
import subprocess
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]


class ManagementCommandTests(unittest.TestCase):
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
            self.invoke(
                "add-source", "orders", csv_path, "id", "amount"
            ).returncode,
            0,
        )
        self.assertEqual(
            self.invoke("add-mapping", "orders", "order_id", "id").returncode, 0
        )
        self.assertEqual(
            self.invoke("add-mapping", "orders", "total", "amount").returncode, 0
        )

    def raw_sources(self) -> list[tuple]:
        conn = sqlite3.connect(self.db_path)
        try:
            return conn.execute(
                "SELECT name, csv_path, retired FROM sources ORDER BY rowid"
            ).fetchall()
        finally:
            conn.close()

    # --- update-source -------------------------------------------------

    def test_update_source_changes_only_path(self) -> None:
        old_path = self.write_csv("old.csv", "order_id,total\nA1,10\n")
        new_path = self.write_csv("new.csv", "order_id,total\nB2,20\n")
        self.register_orders(old_path)
        self.assertEqual(self.invoke("import", "orders").returncode, 0)

        result = self.invoke("update-source", "orders", new_path)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(
            result.stdout.strip(), f"Result: update-source orders {new_path}"
        )
        self.assertEqual(result.stderr, "")

        # 字段与映射不变，路径更新，历史批次仍在。
        self.assertEqual(
            self.invoke("list-sources").stdout.splitlines(),
            [f"orders\t{new_path}\tid,amount"],
        )
        self.assertEqual(
            self.invoke("list-mappings", "orders").stdout.splitlines(),
            ["order_id\tid", "total\tamount"],
        )
        self.assertEqual(
            self.invoke("batches", "orders").stdout.splitlines(),
            ["1\t1\t1\t0\t0\t1", "Result: batches orders"],
        )

    def test_update_source_makes_import_read_new_path(self) -> None:
        old_path = self.write_csv("old.csv", "order_id,total\nA1,10\n")
        new_path = self.write_csv("new.csv", "order_id,total\nB2,20\nC3,30\n")
        self.register_orders(old_path)
        self.assertEqual(self.invoke("import", "orders").returncode, 0)
        self.assertEqual(
            self.invoke("update-source", "orders", new_path).returncode, 0
        )
        # 新批次读新路径：A1 的旧签名不受影响，B2/C3 均为增量。
        result = self.invoke("import", "orders")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "Result: import orders 2 2 2 0 0 2")

    def test_update_source_unknown_source_is_error(self) -> None:
        result = self.invoke("update-source", "missing", "a.csv")
        self.assertEqual(result.returncode, 1)
        self.assertIn("missing", result.stderr)
        self.assertNotIn("Result:", result.stdout)

    def test_update_source_blank_path_is_rejected_and_kept_original(self) -> None:
        old_path = self.write_csv("old.csv", "order_id,total\nA1,10\n")
        self.register_orders(old_path)
        for bad in ("", "   ", "\t", "  \n "):
            with self.subTest(bad=bad):
                result = self.invoke("update-source", "orders", bad)
                self.assertEqual(result.returncode, 1)
                self.assertNotEqual(result.stderr, "")
                self.assertNotIn("Result:", result.stdout)
        self.assertEqual(
            self.invoke("list-sources").stdout.splitlines(),
            [f"orders\t{old_path}\tid,amount"],
        )

    def test_update_source_rejects_reserved_literal(self) -> None:
        old_path = self.write_csv("old.csv", "order_id,total\nA1,10\n")
        self.register_orders(old_path)
        result = self.invoke("update-source", "orders", "TRUE")
        self.assertEqual(result.returncode, 1)
        self.assertIn("TRUE", result.stderr)
        self.assertIn(old_path, self.invoke("list-sources").stdout)

    def test_update_source_missing_argument_is_usage_error(self) -> None:
        result = self.invoke("update-source", "orders")
        self.assertEqual(result.returncode, 2)
        self.assertNotEqual(result.stderr, "")
        self.assertEqual(result.stdout, "")

    # --- remove-mapping ------------------------------------------------

    def test_remove_mapping_deletes_one_mapping(self) -> None:
        csv_path = self.write_csv("a.csv", "order_id,total\nA1,10\n")
        self.register_orders(csv_path)
        result = self.invoke("remove-mapping", "orders", "total")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "Result: remove-mapping orders total")
        self.assertEqual(
            self.invoke("list-mappings", "orders").stdout.splitlines(),
            ["order_id\tid"],
        )

    def test_remove_mapping_unknown_column_is_error_without_changes(self) -> None:
        csv_path = self.write_csv("a.csv", "order_id,total\nA1,10\n")
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
        result = self.invoke("remove-mapping", "missing", "a")
        self.assertEqual(result.returncode, 1)
        self.assertIn("missing", result.stderr)

    def test_remove_mapping_missing_argument_is_usage_error(self) -> None:
        result = self.invoke("remove-mapping", "orders")
        self.assertEqual(result.returncode, 2)
        self.assertNotEqual(result.stderr, "")
        self.assertEqual(result.stdout, "")

    # --- retire-source -------------------------------------------------

    def test_retire_deletes_source_fields_and_mappings_keeps_history(self) -> None:
        csv_path = self.write_csv(
            "orders.csv", "order_id,total\nA1,10\nA2,\n"
        )
        self.register_orders(csv_path)
        self.assertEqual(self.invoke("import", "orders").returncode, 0)
        reject_file = self.work / "rejected_orders_1.csv"
        self.assertTrue(reject_file.is_file())

        result = self.invoke("retire-source", "orders")
        self.assertEqual(result.returncode, 0, result.stderr)
        # 2 个必需字段 + 2 条映射 = 4。
        self.assertEqual(result.stdout.strip(), "Result: retire-source orders 4")

        self.assertEqual(self.invoke("list-sources").stdout, "")
        for command in ("list-mappings", "batches"):
            failed = self.invoke(command, "orders")
            self.assertEqual(failed.returncode, 1)
            self.assertIn("orders", failed.stderr)
        self.assertEqual(
            self.invoke("update-source", "orders", csv_path).returncode, 1
        )
        self.assertEqual(
            self.invoke("remove-mapping", "orders", "total").returncode, 1
        )

        # 历史批次与签名仍在库中（挂到内部墓碑来源），拒绝文件仍在磁盘上。
        conn = sqlite3.connect(self.db_path)
        try:
            self.assertEqual(
                conn.execute(
                    "SELECT COUNT(*) FROM import_batches"
                ).fetchone()[0],
                1,
            )
            self.assertEqual(
                conn.execute(
                    "SELECT COUNT(*) FROM imported_records"
                ).fetchone()[0],
                1,
            )
            self.assertEqual(
                conn.execute(
                    "SELECT COUNT(*) FROM sources WHERE name = 'orders'"
                ).fetchone()[0],
                0,
            )
            self.assertEqual(
                conn.execute(
                    "SELECT COUNT(*) FROM source_fields WHERE source_name = 'orders'"
                ).fetchone()[0],
                0,
            )
            self.assertEqual(
                conn.execute(
                    "SELECT COUNT(*) FROM field_mappings WHERE source_name = 'orders'"
                ).fetchone()[0],
                0,
            )
        finally:
            conn.close()
        self.assertTrue(reject_file.is_file())

    def test_retire_allows_reregister_with_batch_numbers_restarting(self) -> None:
        csv_path = self.write_csv("orders.csv", "order_id,total\nA1,10\n")
        self.register_orders(csv_path)
        self.assertEqual(self.invoke("import", "orders").returncode, 0)
        self.assertEqual(self.invoke("retire-source", "orders").returncode, 0)

        # 同名按 add-source 规则重新注册：相同行不再判重，批次号从 1 开始。
        self.register_orders(csv_path)
        result = self.invoke("import", "orders")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "Result: import orders 1 1 1 0 0 1")

    def test_retire_twice_keeps_each_generation_separate(self) -> None:
        csv_path = self.write_csv("orders.csv", "order_id,total\nA1,10\n")
        for generation in range(2):
            self.register_orders(csv_path)
            self.assertEqual(self.invoke("import", "orders").returncode, 0)
            result = self.invoke("retire-source", "orders")
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(
                result.stdout.strip(), "Result: retire-source orders 4"
            )
        # 两代历史批次各保留一条，互不干扰。
        conn = sqlite3.connect(self.db_path)
        try:
            self.assertEqual(
                conn.execute("SELECT COUNT(*) FROM import_batches").fetchone()[0], 2
            )
            self.assertEqual(
                conn.execute("SELECT COUNT(*) FROM imported_records").fetchone()[0],
                2,
            )
            self.assertEqual(
                conn.execute(
                    "SELECT COUNT(*) FROM sources WHERE retired = 1"
                ).fetchone()[0],
                2,
            )
        finally:
            conn.close()

    def test_retire_unknown_source_is_error_without_changes(self) -> None:
        result = self.invoke("retire-source", "missing")
        self.assertEqual(result.returncode, 1)
        self.assertIn("missing", result.stderr)
        self.assertNotIn("Result:", result.stdout)
        self.assertEqual(self.raw_sources(), [])

    def test_retire_missing_argument_is_usage_error(self) -> None:
        result = self.invoke("retire-source")
        self.assertEqual(result.returncode, 2)
        self.assertNotEqual(result.stderr, "")
        self.assertEqual(result.stdout, "")


if __name__ == "__main__":
    unittest.main()
