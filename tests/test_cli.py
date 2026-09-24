"""Checks for the documented command-line entry point."""

from pathlib import Path
import subprocess
import sys
import unittest


ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "import_ledger.db"


class CommandLineTests(unittest.TestCase):
    def invoke(self, *arguments: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [sys.executable, "-m", "import_pipeline", *arguments],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=False,
        )

    def setUp(self) -> None:
        DB_PATH.unlink(missing_ok=True)

    def tearDown(self) -> None:
        DB_PATH.unlink(missing_ok=True)

    def test_help_and_no_arguments(self) -> None:
        for arguments in [(), ("--help",)]:
            with self.subTest(arguments=arguments):
                result = self.invoke(*arguments)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertIn("--help", result.stdout)
                self.assertIn("--version", result.stdout)
                self.assertEqual(result.stderr, "")

    def test_version(self) -> None:
        result = self.invoke("--version")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "import-pipeline 0.1.0")
        self.assertEqual(result.stderr, "")

    def test_unknown_argument_is_an_error(self) -> None:
        result = self.invoke("--unknown-option")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("--unknown-option", result.stderr)
        self.assertEqual(result.stdout, "")

    def test_unknown_subcommand_is_an_error(self) -> None:
        result = self.invoke("frobnicate")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("frobnicate", result.stderr)
        self.assertEqual(result.stdout, "")

    def test_missing_required_argument_is_an_error(self) -> None:
        result = self.invoke("add-source", "--name", "s1")
        self.assertEqual(result.returncode, 2)
        self.assertIn("--path", result.stderr)
        self.assertEqual(result.stdout, "")

    def test_add_source_and_list(self) -> None:
        result = self.invoke(
            "add-source",
            "--name",
            "sales",
            "--path",
            "data/sales.csv",
            "--field",
            "id",
            "--field",
            "amount",
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "Result: add-source sales 1")
        self.assertEqual(result.stderr, "")

        listing = self.invoke("list-sources")
        self.assertEqual(listing.returncode, 0, listing.stderr)
        self.assertEqual(
            listing.stdout, "sales\tdata/sales.csv\tid,amount\n"
        )

    def test_duplicate_source_is_rejected_and_keeps_first(self) -> None:
        first = self.invoke(
            "add-source", "--name", "s", "--path", "a.csv", "--field", "x"
        )
        self.assertEqual(first.returncode, 0, first.stderr)

        second = self.invoke(
            "add-source", "--name", "s", "--path", "b.csv", "--field", "y"
        )
        self.assertNotEqual(second.returncode, 0)
        self.assertNotEqual(second.returncode, 2)
        self.assertIn("s", second.stderr)
        self.assertEqual(second.stdout, "")

        listing = self.invoke("list-sources")
        self.assertEqual(listing.stdout, "s\ta.csv\tx\n")

    def test_duplicate_field_name_is_rejected_without_persistence(self) -> None:
        result = self.invoke(
            "add-source",
            "--name",
            "s",
            "--path",
            "a.csv",
            "--field",
            "x",
            "--field",
            "x",
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertNotEqual(result.returncode, 2)
        self.assertIn("x", result.stderr)
        self.assertEqual(result.stdout, "")
        self.assertEqual(self.invoke("list-sources").stdout, "")

    def test_empty_field_name_is_rejected(self) -> None:
        result = self.invoke(
            "add-source", "--name", "s", "--path", "a.csv", "--field", ""
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(result.stdout, "")
        self.assertEqual(self.invoke("list-sources").stdout, "")

    def test_add_mapping_and_list_in_registration_order(self) -> None:
        setup = self.invoke(
            "add-source", "--name", "s", "--path", "a.csv", "--field", "x"
        )
        self.assertEqual(setup.returncode, 0, setup.stderr)

        first = self.invoke(
            "add-mapping",
            "--source",
            "s",
            "--source-column",
            "col_a",
            "--target-column",
            "field_a",
        )
        self.assertEqual(first.returncode, 0, first.stderr)
        self.assertEqual(first.stdout.strip(), "Result: add-mapping s 1")

        second = self.invoke(
            "add-mapping",
            "--source",
            "s",
            "--source-column",
            "col_b",
            "--target-column",
            "field_b",
        )
        self.assertEqual(second.returncode, 0, second.stderr)

        listing = self.invoke("list-mappings", "--source", "s")
        self.assertEqual(
            listing.stdout, "col_a\tfield_a\ncol_b\tfield_b\n"
        )

    def test_duplicate_source_column_keeps_first_mapping(self) -> None:
        self.invoke(
            "add-source", "--name", "s", "--path", "a.csv", "--field", "x"
        )
        self.invoke(
            "add-mapping",
            "--source",
            "s",
            "--source-column",
            "col_a",
            "--target-column",
            "field_a",
        )
        conflict = self.invoke(
            "add-mapping",
            "--source",
            "s",
            "--source-column",
            "col_a",
            "--target-column",
            "field_other",
        )
        self.assertNotEqual(conflict.returncode, 0)
        self.assertNotEqual(conflict.returncode, 2)
        self.assertIn("col_a", conflict.stderr)
        self.assertEqual(conflict.stdout, "")

        listing = self.invoke("list-mappings", "--source", "s")
        self.assertEqual(listing.stdout, "col_a\tfield_a\n")

    def test_mapping_unknown_source_is_rejected(self) -> None:
        result = self.invoke(
            "add-mapping",
            "--source",
            "ghost",
            "--source-column",
            "a",
            "--target-column",
            "b",
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertNotEqual(result.returncode, 2)
        self.assertIn("ghost", result.stderr)
        self.assertEqual(result.stdout, "")

    def test_empty_mapping_value_is_rejected(self) -> None:
        self.invoke(
            "add-source", "--name", "s", "--path", "a.csv", "--field", "x"
        )
        result = self.invoke(
            "add-mapping",
            "--source",
            "s",
            "--source-column",
            "",
            "--target-column",
            "b",
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(result.stdout, "")
        self.assertEqual(self.invoke("list-mappings", "--source", "s").stdout, "")

    def test_list_sources_empty_database_outputs_nothing(self) -> None:
        result = self.invoke("list-sources")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout, "")

    def test_list_mappings_unknown_source_is_an_error(self) -> None:
        result = self.invoke("list-mappings", "--source", "ghost")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("ghost", result.stderr)
        self.assertEqual(result.stdout, "")

    def test_only_database_file_is_created(self) -> None:
        self.invoke(
            "add-source", "--name", "s", "--path", "a.csv", "--field", "x"
        )
        business_files = [
            p.name
            for p in ROOT.iterdir()
            if p.is_file()
            and not p.name.startswith(".")
            and p.suffix not in {".py", ".md"}
            and p.name != "import_ledger.db"
        ]
        self.assertEqual(business_files, [])


if __name__ == "__main__":
    unittest.main()
