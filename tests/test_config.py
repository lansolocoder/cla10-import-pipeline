"""Checks for the configuration subcommands backed by the SQLite ledger."""

from pathlib import Path
import os
import subprocess
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]


class ConfigCommandTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.db_path = Path(self.tmp.name) / "import_ledger.db"

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

    def test_add_source_and_list_sources(self) -> None:
        result = self.invoke("add-source", "orders", "data/orders.csv", "id", "amount")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "Result: add-source orders 2")

        result = self.invoke("add-source", "customers", "data/customers.csv", "email")
        self.assertEqual(result.returncode, 0, result.stderr)

        result = self.invoke("list-sources")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(
            result.stdout.splitlines(),
            [
                "orders\tdata/orders.csv\tid,amount",
                "customers\tdata/customers.csv\temail",
            ],
        )

    def test_add_source_rejects_duplicate_name_and_keeps_original(self) -> None:
        self.assertEqual(
            self.invoke("add-source", "orders", "a.csv", "id").returncode, 0
        )
        result = self.invoke("add-source", "orders", "b.csv", "other")
        self.assertNotEqual(result.returncode, 0)
        self.assertNotEqual(result.returncode, 2)
        self.assertIn("orders", result.stderr)
        self.assertNotIn("Result:", result.stdout)

        result = self.invoke("list-sources")
        self.assertEqual(result.stdout.splitlines(), ["orders\ta.csv\tid"])

    def test_add_source_rejects_invalid_fields(self) -> None:
        for fields in [("id", "id"), ("",), ("TRUE",)]:
            with self.subTest(fields=fields):
                result = self.invoke("add-source", "orders", "a.csv", *fields)
                self.assertNotEqual(result.returncode, 0)
                self.assertNotEqual(result.stderr, "")
        result = self.invoke("list-sources")
        self.assertEqual(result.returncode, 0)
        self.assertEqual(result.stdout, "")

    def test_add_mapping_and_list_mappings(self) -> None:
        self.assertEqual(
            self.invoke("add-source", "orders", "a.csv", "id").returncode, 0
        )
        result = self.invoke("add-mapping", "orders", "order_id", "id")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "Result: add-mapping orders 1")
        self.assertEqual(
            self.invoke("add-mapping", "orders", "total", "amount").returncode, 0
        )

        result = self.invoke("list-mappings", "orders")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(
            result.stdout.splitlines(), ["order_id\tid", "total\tamount"]
        )

    def test_add_mapping_conflict_keeps_first(self) -> None:
        self.assertEqual(
            self.invoke("add-source", "orders", "a.csv", "id").returncode, 0
        )
        self.assertEqual(
            self.invoke("add-mapping", "orders", "order_id", "id").returncode, 0
        )
        result = self.invoke("add-mapping", "orders", "order_id", "other")
        self.assertNotEqual(result.returncode, 0)
        self.assertNotIn("Result:", result.stdout)

        result = self.invoke("list-mappings", "orders")
        self.assertEqual(result.stdout.splitlines(), ["order_id\tid"])

    def test_add_mapping_unknown_source_is_rejected(self) -> None:
        result = self.invoke("add-mapping", "missing", "a", "b")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("missing", result.stderr)
        self.assertNotIn("Result:", result.stdout)

    def test_list_mappings_unknown_source_is_rejected(self) -> None:
        result = self.invoke("list-mappings", "missing")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("missing", result.stderr)

    def test_list_mappings_empty_is_success(self) -> None:
        self.assertEqual(
            self.invoke("add-source", "orders", "a.csv", "id").returncode, 0
        )
        result = self.invoke("list-mappings", "orders")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout, "")

    def test_missing_arguments_are_usage_errors(self) -> None:
        result = self.invoke("add-source", "orders")
        self.assertNotEqual(result.returncode, 0)
        self.assertNotEqual(result.stderr, "")

    def test_unknown_subcommand_is_an_error(self) -> None:
        result = self.invoke("frobnicate")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("frobnicate", result.stderr)


if __name__ == "__main__":
    unittest.main()
