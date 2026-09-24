"""Checks for run-import and show-batch subcommands."""

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
        self.dir = Path(self.tmp.name)
        self.db_path = self.dir / "import_ledger.db"

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

    def write_csv(self, name: str, content: str) -> Path:
        path = self.dir / name
        path.write_text(content, encoding="utf-8")
        return path

    def register_orders(self, csv_path: Path | str) -> None:
        self.assertEqual(
            self.invoke(
                "add-source", "orders", str(csv_path), "id", "amount"
            ).returncode,
            0,
        )
        self.assertEqual(
            self.invoke("add-mapping", "orders", "order_id", "id").returncode, 0
        )
        self.assertEqual(
            self.invoke("add-mapping", "orders", "total", "amount").returncode, 0
        )

    def record_count(self) -> int:
        conn = sqlite3.connect(self.db_path)
        try:
            return conn.execute("SELECT COUNT(*) FROM imported_records").fetchone()[0]
        finally:
            conn.close()

    def test_successful_import_and_show_batch(self) -> None:
        csv_path = self.write_csv(
            "orders.csv", "order_id,total\n100,9.5\n101,12\n"
        )
        self.register_orders(csv_path)

        result = self.invoke("run-import", "orders")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "Result: run-import orders 2")
        self.assertEqual(result.stderr, "")

        result = self.invoke("show-batch", "orders", "1")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.splitlines(), ["1\tok\t2\t0"])
        self.assertEqual(self.record_count(), 4)  # 2 行 × 2 个目标列

    def test_missing_csv_leaves_no_batch(self) -> None:
        missing = self.dir / "nope.csv"
        self.register_orders(missing)

        result = self.invoke("run-import", "orders")
        self.assertEqual(result.returncode, 1)
        self.assertTrue(result.stderr.startswith("Error:"))
        self.assertNotIn("Result:", result.stdout)

        result = self.invoke("show-batch", "orders", "1")
        self.assertEqual(result.returncode, 1)
        self.assertTrue(result.stderr.startswith("Error:"))
        self.assertEqual(result.stdout, "")

    def test_unmapped_source_column_rejects_whole_batch(self) -> None:
        csv_path = self.write_csv(
            "orders.csv", "order_id,total,note\n100,9.5,hi\n101,12,yo\n"
        )
        self.register_orders(csv_path)

        result = self.invoke("run-import", "orders")
        self.assertEqual(result.returncode, 1)
        self.assertTrue(result.stderr.startswith("Error:"))
        self.assertEqual(self.record_count(), 0)

        result = self.invoke("show-batch", "orders", "1")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.splitlines(), ["1\trejected\t0\t2"])

    def test_missing_mapping_target_rejects_whole_batch(self) -> None:
        csv_path = self.write_csv("orders.csv", "order_id\n100\n101\n")
        self.assertEqual(
            self.invoke(
                "add-source", "orders", str(csv_path), "id", "amount"
            ).returncode,
            0,
        )
        self.assertEqual(
            self.invoke("add-mapping", "orders", "order_id", "id").returncode, 0
        )

        result = self.invoke("run-import", "orders")
        self.assertEqual(result.returncode, 1)
        self.assertTrue(result.stderr.startswith("Error:"))
        self.assertEqual(self.record_count(), 0)

        result = self.invoke("show-batch", "orders", "1")
        self.assertEqual(result.stdout.splitlines(), ["1\trejected\t0\t2"])

    def test_blank_value_rejects_and_keeps_validated_rows_out(self) -> None:
        csv_path = self.write_csv(
            "orders.csv", "order_id,total\n100,9.5\n101,\n"
        )
        self.register_orders(csv_path)

        result = self.invoke("run-import", "orders")
        self.assertEqual(result.returncode, 1)
        self.assertTrue(result.stderr.startswith("Error:"))
        self.assertEqual(self.record_count(), 0)

        # 修正后重跑为新批次，全部行一次性落库。
        csv_path.write_text("order_id,total\n100,9.5\n101,12\n", encoding="utf-8")
        result = self.invoke("run-import", "orders")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "Result: run-import orders 2")
        self.assertEqual(self.record_count(), 4)

    def test_repeated_runs_get_incrementing_batches_and_keep_history(self) -> None:
        csv_path = self.write_csv("orders.csv", "order_id,total\n100,9.5\n")
        self.register_orders(csv_path)

        self.assertEqual(self.invoke("run-import", "orders").returncode, 0)
        # 制造一次 rejected 批次。
        csv_path.write_text("order_id,total,note\n100,9.5,x\n", encoding="utf-8")
        self.assertNotEqual(self.invoke("run-import", "orders").returncode, 0)
        # 再次成功。
        csv_path.write_text("order_id,total\n100,9.5\n200,3\n", encoding="utf-8")
        self.assertEqual(self.invoke("run-import", "orders").returncode, 0)

        result = self.invoke("show-batch", "orders", "2")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(
            result.stdout.splitlines(),
            [
                "1\tok\t1\t0",
                "2\trejected\t0\t1",
                "3\tok\t2\t0",
            ],
        )

    def test_show_batch_unknown_source_or_number(self) -> None:
        csv_path = self.write_csv("orders.csv", "order_id,total\n100,9.5\n")
        self.register_orders(csv_path)
        self.assertEqual(self.invoke("run-import", "orders").returncode, 0)

        result = self.invoke("show-batch", "ghost", "1")
        self.assertEqual(result.returncode, 1)
        self.assertTrue(result.stderr.startswith("Error:"))
        self.assertEqual(result.stdout, "")

        result = self.invoke("show-batch", "orders", "99")
        self.assertEqual(result.returncode, 1)
        self.assertTrue(result.stderr.startswith("Error:"))
        self.assertEqual(result.stdout, "")

    def test_run_import_unknown_source(self) -> None:
        result = self.invoke("run-import", "ghost")
        self.assertEqual(result.returncode, 1)
        self.assertTrue(result.stderr.startswith("Error:"))


if __name__ == "__main__":
    unittest.main()
