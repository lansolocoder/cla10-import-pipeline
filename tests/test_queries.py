"""Checks for the show-rows and find-dupes read-only subcommands."""

from pathlib import Path
import contextlib
import hashlib
import io
import os
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

from import_pipeline import cli, ledger


ROOT = Path(__file__).resolve().parents[1]


class ReadonlyQueryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.db_path = Path(self.tmp.name) / "import_ledger.db"
        self.csv_path = Path(self.tmp.name) / "orders.csv"

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

    def write_csv(self, text: str) -> None:
        self.csv_path.write_text(text, encoding="utf-8")

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

    def db_digest(self) -> str:
        return hashlib.sha256(self.db_path.read_bytes()).hexdigest()

    def assert_readonly(self, result: subprocess.CompletedProcess[str]) -> None:
        self.assertEqual(result.stderr, "")
        self.assertEqual(self.db_digest(), self.digest_before)

    def import_rows(self, rows: str) -> None:
        self.write_csv(rows)
        result = self.invoke("run-import", "orders")
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_show_rows_outputs_normalized_json_by_row_number(self) -> None:
        self.register_orders()
        self.import_rows("order_id,total\n1,10\n2,20\n")
        self.digest_before = self.db_digest()
        result = self.invoke("show-rows", "orders", "1")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(
            result.stdout.splitlines(),
            [
                '1\t{"amount": "10", "id": "1"}',
                '2\t{"amount": "20", "id": "2"}',
            ],
        )
        self.assert_readonly(result)

    def test_show_rows_rejected_batch_is_empty(self) -> None:
        self.register_orders()
        self.write_csv("order_id,total\n1,\n")
        self.assertEqual(self.invoke("run-import", "orders").returncode, 1)
        self.digest_before = self.db_digest()
        result = self.invoke("show-rows", "orders", "1")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout, "")
        self.assert_readonly(result)

    def test_show_rows_revoked_batch_is_empty(self) -> None:
        self.register_orders()
        self.import_rows("order_id,total\n1,10\n")
        self.assertEqual(self.invoke("revoke-batch", "orders", "1").returncode, 0)
        self.digest_before = self.db_digest()
        result = self.invoke("show-rows", "orders", "1")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout, "")
        self.assert_readonly(result)

    def test_show_rows_other_batch_revoke_does_not_affect_output(self) -> None:
        self.register_orders()
        self.import_rows("order_id,total\n1,10\n2,20\n")
        self.import_rows("order_id,total\n3,30\n")
        self.assertEqual(self.invoke("revoke-batch", "orders", "2").returncode, 0)
        result = self.invoke("show-rows", "orders", "1")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(
            result.stdout.splitlines(),
            ['1\t{"amount": "10", "id": "1"}', '2\t{"amount": "20", "id": "2"}'],
        )

    def test_show_rows_unknown_source_or_batch_is_rejected(self) -> None:
        self.register_orders()
        self.import_rows("order_id,total\n1,10\n")
        self.digest_before = self.db_digest()
        for arguments in [("missing", "1"), ("orders", "9")]:
            with self.subTest(arguments=arguments):
                result = self.invoke("show-rows", *arguments)
                self.assertEqual(result.returncode, 1)
                self.assertTrue(result.stderr.startswith("Error:"))
                self.assertEqual(len(result.stderr.strip().splitlines()), 1)
                self.assertEqual(result.stdout, "")
                self.assertEqual(self.db_digest(), self.digest_before)

    def test_show_rows_rejects_invalid_arguments(self) -> None:
        self.register_orders()
        self.import_rows("order_id,total\n1,10\n")
        self.digest_before = self.db_digest()
        for arguments in [("orders", "0"), ("orders", "-1"), ("TRUE", "1")]:
            with self.subTest(arguments=arguments):
                result = self.invoke("show-rows", *arguments)
                self.assertEqual(result.returncode, 1)
                self.assertTrue(result.stderr.startswith("Error:"))
                self.assertEqual(result.stdout, "")
                self.assertEqual(self.db_digest(), self.digest_before)

    def test_find_dupes_merges_rows_across_batches(self) -> None:
        self.register_orders()
        self.import_rows("order_id,total\n1,10\n2,20\n")
        self.import_rows("order_id,total\n3,10\n4,30\n")
        self.digest_before = self.db_digest()
        result = self.invoke("find-dupes", "orders", "amount")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.splitlines(), ["10\t2\t1,2"])
        self.assert_readonly(result)

    def test_find_dupes_excludes_revoked_batch_rows(self) -> None:
        self.register_orders()
        self.import_rows("order_id,total\n1,10\n2,20\n")
        self.import_rows("order_id,total\n3,10\n4,20\n")
        self.assertEqual(self.invoke("revoke-batch", "orders", "1").returncode, 0)
        result = self.invoke("find-dupes", "orders", "amount")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout, "")

    def test_find_dupes_no_duplicates_is_empty(self) -> None:
        self.register_orders()
        self.import_rows("order_id,total\n1,10\n2,20\n")
        result = self.invoke("find-dupes", "orders", "amount")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout, "")

    def test_find_dupes_groups_sorted_by_value(self) -> None:
        self.register_orders()
        self.import_rows("order_id,total\n1,20\n2,10\n")
        self.import_rows("order_id,total\n3,20\n4,10\n")
        result = self.invoke("find-dupes", "orders", "amount")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(
            result.stdout.splitlines(), ["10\t2\t1,2", "20\t2\t1,2"]
        )

    def test_find_dupes_unknown_target_column_is_rejected(self) -> None:
        self.register_orders()
        self.import_rows("order_id,total\n1,10\n")
        self.digest_before = self.db_digest()
        result = self.invoke("find-dupes", "orders", "missing")
        self.assertEqual(result.returncode, 1)
        self.assertTrue(result.stderr.startswith("Error:"))
        self.assertEqual(len(result.stderr.strip().splitlines()), 1)
        self.assertEqual(result.stdout, "")
        self.assertEqual(self.db_digest(), self.digest_before)

    def test_find_dupes_rejects_invalid_arguments(self) -> None:
        self.register_orders()
        self.import_rows("order_id,total\n1,10\n")
        self.digest_before = self.db_digest()
        for arguments in [("missing", "amount"), ("TRUE", "amount"), ("orders", "  ")]:
            with self.subTest(arguments=arguments):
                result = self.invoke("find-dupes", *arguments)
                self.assertEqual(result.returncode, 1)
                self.assertTrue(result.stderr.startswith("Error:"))
                self.assertEqual(result.stdout, "")
                self.assertEqual(self.db_digest(), self.digest_before)

    def test_batch_stats_summarizes_ok_batch(self) -> None:
        self.register_orders()
        self.import_rows("order_id,total\n1,10\n2,20\n")
        self.digest_before = self.db_digest()
        result = self.invoke("batch-stats", "orders", "1", "1")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.splitlines(), ["1\t1\t0\t0\t2\t0"])
        self.assert_readonly(result)

    def test_batch_stats_matches_revoke_then_reject_scenario(self) -> None:
        self.register_orders()
        self.import_rows("order_id,total\n1,10\n2,20\n")
        self.write_csv("order_id,total\n3,\n")
        self.assertEqual(self.invoke("run-import", "orders").returncode, 1)
        self.assertEqual(self.invoke("revoke-batch", "orders", "1").returncode, 0)
        self.digest_before = self.db_digest()
        result = self.invoke("batch-stats", "orders", "1", "2")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.splitlines(), ["2\t0\t1\t1\t2\t1"])
        self.assert_readonly(result)

    def test_batch_stats_range_bounds_are_inclusive(self) -> None:
        self.register_orders()
        self.import_rows("order_id,total\n1,10\n")
        self.import_rows("order_id,total\n2,20\n3,30\n")
        self.import_rows("order_id,total\n4,40\n")
        result = self.invoke("batch-stats", "orders", "2", "2")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.splitlines(), ["1\t1\t0\t0\t2\t0"])
        result = self.invoke("batch-stats", "orders", "2", "9")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.splitlines(), ["2\t2\t0\t0\t3\t0"])

    def test_batch_stats_empty_range_is_all_zeros(self) -> None:
        self.register_orders()
        self.import_rows("order_id,total\n1,10\n")
        self.digest_before = self.db_digest()
        for arguments in [("orders", "2", "3"), ("orders", "9", "100")]:
            with self.subTest(arguments=arguments):
                result = self.invoke("batch-stats", *arguments)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(result.stdout.splitlines(), ["0\t0\t0\t0\t0\t0"])
                self.assertEqual(self.db_digest(), self.digest_before)

    def test_batch_stats_does_not_change_other_query_results(self) -> None:
        self.register_orders()
        self.import_rows("order_id,total\n1,10\n2,20\n")
        self.write_csv("order_id,total\n3,\n")
        self.assertEqual(self.invoke("run-import", "orders").returncode, 1)
        self.assertEqual(self.invoke("revoke-batch", "orders", "1").returncode, 0)
        before = {
            name: self.invoke(*command)
            for name, command in [
                ("show-batch", ("show-batch", "orders", "1")),
                ("show-rows", ("show-rows", "orders", "1")),
                ("find-dupes", ("find-dupes", "orders", "amount")),
            ]
        }
        stats = self.invoke("batch-stats", "orders", "1", "2")
        self.assertEqual(stats.returncode, 0, stats.stderr)
        for name, command in [
            ("show-batch", ("show-batch", "orders", "1")),
            ("show-rows", ("show-rows", "orders", "1")),
            ("find-dupes", ("find-dupes", "orders", "amount")),
        ]:
            after = self.invoke(*command)
            self.assertEqual(
                (after.returncode, after.stdout, after.stderr),
                (
                    before[name].returncode,
                    before[name].stdout,
                    before[name].stderr,
                ),
                name,
            )

    def test_batch_stats_rejects_invalid_arguments(self) -> None:
        self.register_orders()
        self.import_rows("order_id,total\n1,10\n")
        self.digest_before = self.db_digest()
        invalid = [
            ("missing", "1", "2"),
            ("TRUE", "1", "2"),
            ("  ", "1", "2"),
            ("orders", "0", "2"),
            ("orders", "-1", "2"),
            ("orders", "1", "0"),
            ("orders", "2", "-3"),
            ("orders", "abc", "2"),
            ("orders", "1", "2.5"),
            ("orders", "2", "1"),
        ]
        for arguments in invalid:
            with self.subTest(arguments=arguments):
                result = self.invoke("batch-stats", *arguments)
                self.assertEqual(result.returncode, 1)
                self.assertTrue(result.stderr.startswith("Error:"))
                self.assertEqual(len(result.stderr.strip().splitlines()), 1)
                self.assertEqual(result.stdout, "")
                self.assertEqual(self.db_digest(), self.digest_before)

    def test_batch_stats_detail_lists_matching_status_batches(self) -> None:
        self.register_orders()
        self.import_rows("order_id,total\n1,10\n2,20\n")
        self.import_rows("order_id,total\n3,30\n4,40\n5,50\n")
        self.digest_before = self.db_digest()
        result = self.invoke("batch-stats-detail", "orders", "1", "2", "ok")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(
            result.stdout.splitlines(), ["1\t2\t0", "2\t3\t0"]
        )
        self.assert_readonly(result)

    def test_batch_stats_detail_revoke_then_reject_scenario(self) -> None:
        self.register_orders()
        self.import_rows("order_id,total\n1,10\n2,20\n")
        self.write_csv("order_id,total\n3,\n")
        self.assertEqual(self.invoke("run-import", "orders").returncode, 1)
        self.assertEqual(self.invoke("revoke-batch", "orders", "1").returncode, 0)
        self.digest_before = self.db_digest()
        revoked = self.invoke("batch-stats-detail", "orders", "1", "2", "revoked")
        self.assertEqual(revoked.returncode, 0, revoked.stderr)
        self.assertEqual(revoked.stdout.splitlines(), ["1\t2\t0"])
        rejected = self.invoke("batch-stats-detail", "orders", "1", "2", "rejected")
        self.assertEqual(rejected.returncode, 0, rejected.stderr)
        self.assertEqual(rejected.stdout.splitlines(), ["2\t0\t1"])
        ok = self.invoke("batch-stats-detail", "orders", "1", "2", "ok")
        self.assertEqual(ok.returncode, 0, ok.stderr)
        self.assertEqual(ok.stdout, "")
        self.assertEqual(self.db_digest(), self.digest_before)

    def test_batch_stats_detail_range_bounds_are_inclusive(self) -> None:
        self.register_orders()
        self.import_rows("order_id,total\n1,10\n")
        self.import_rows("order_id,total\n2,20\n")
        self.import_rows("order_id,total\n3,30\n")
        result = self.invoke("batch-stats-detail", "orders", "2", "2", "ok")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.splitlines(), ["2\t1\t0"])
        result = self.invoke("batch-stats-detail", "orders", "2", "9", "ok")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(
            result.stdout.splitlines(), ["2\t1\t0", "3\t1\t0"]
        )

    def test_batch_stats_detail_no_matching_status_is_empty(self) -> None:
        self.register_orders()
        self.import_rows("order_id,total\n1,10\n")
        self.write_csv("order_id,total\n2,\n")
        self.assertEqual(self.invoke("run-import", "orders").returncode, 1)
        self.digest_before = self.db_digest()
        for arguments in [
            ("orders", "1", "2", "revoked"),
            ("orders", "1", "1", "rejected"),
            ("orders", "9", "100", "ok"),
        ]:
            with self.subTest(arguments=arguments):
                result = self.invoke("batch-stats-detail", *arguments)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(result.stdout, "")
                self.assertEqual(self.db_digest(), self.digest_before)

    def test_batch_stats_detail_totals_match_batch_stats(self) -> None:
        self.register_orders()
        self.import_rows("order_id,total\n1,10\n2,20\n")
        self.import_rows("order_id,total\n3,30\n4,40\n")
        self.write_csv("order_id,total\n5,\n6,\n")
        self.assertEqual(self.invoke("run-import", "orders").returncode, 1)
        self.assertEqual(self.invoke("revoke-batch", "orders", "1").returncode, 0)
        stats = self.invoke("batch-stats", "orders", "1", "3")
        self.assertEqual(stats.returncode, 0, stats.stderr)
        total, ok_count, rejected_count, revoked_count, succeeded_sum, quarantined_sum = (
            int(value) for value in stats.stdout.split("\t")
        )
        counts = {"ok": ok_count, "rejected": rejected_count, "revoked": revoked_count}
        succeeded_agg = quarantined_agg = detail_rows = 0
        for status in ("ok", "rejected", "revoked"):
            result = self.invoke("batch-stats-detail", "orders", "1", "3", status)
            self.assertEqual(result.returncode, 0, result.stderr)
            lines = result.stdout.splitlines()
            self.assertEqual(len(lines), counts[status])
            detail_rows += len(lines)
            for line in lines:
                succeeded, quarantined = (int(value) for value in line.split("\t")[1:])
                succeeded_agg += succeeded
                quarantined_agg += quarantined
        self.assertEqual(detail_rows, total)
        self.assertEqual(succeeded_agg, succeeded_sum)
        self.assertEqual(quarantined_agg, quarantined_sum)

    def test_batch_stats_detail_does_not_change_other_query_results(self) -> None:
        self.register_orders()
        self.import_rows("order_id,total\n1,10\n2,20\n")
        self.write_csv("order_id,total\n3,\n")
        self.assertEqual(self.invoke("run-import", "orders").returncode, 1)
        self.assertEqual(self.invoke("revoke-batch", "orders", "1").returncode, 0)
        commands = [
            ("show-batch", "orders", "1"),
            ("show-rows", "orders", "1"),
            ("find-dupes", "orders", "amount"),
            ("batch-stats", "orders", "1", "2"),
        ]
        before = {
            command: self.invoke(*command) for command in commands
        }
        detail = self.invoke("batch-stats-detail", "orders", "1", "2", "revoked")
        self.assertEqual(detail.returncode, 0, detail.stderr)
        for command in commands:
            after = self.invoke(*command)
            self.assertEqual(
                (after.returncode, after.stdout, after.stderr),
                (
                    before[command].returncode,
                    before[command].stdout,
                    before[command].stderr,
                ),
                command,
            )

    def test_batch_stats_detail_rejects_invalid_arguments(self) -> None:
        self.register_orders()
        self.import_rows("order_id,total\n1,10\n")
        self.digest_before = self.db_digest()
        invalid = [
            ("missing", "1", "2", "ok"),
            ("TRUE", "1", "2", "ok"),
            ("  ", "1", "2", "ok"),
            ("orders", "0", "2", "ok"),
            ("orders", "-1", "2", "ok"),
            ("orders", "1", "0", "ok"),
            ("orders", "2", "-3", "ok"),
            ("orders", "abc", "2", "ok"),
            ("orders", "1", "2.5", "ok"),
            ("orders", "2", "1", "ok"),
            ("orders", "1", "2", "OK"),
            ("orders", "1", "2", "failed"),
            ("orders", "1", "2", ""),
            ("orders", "1", "2", "ok "),
        ]
        for arguments in invalid:
            with self.subTest(arguments=arguments):
                result = self.invoke("batch-stats-detail", *arguments)
                self.assertEqual(result.returncode, 1)
                self.assertTrue(result.stderr.startswith("Error:"))
                self.assertEqual(len(result.stderr.strip().splitlines()), 1)
                self.assertEqual(result.stdout, "")
                self.assertEqual(self.db_digest(), self.digest_before)


    def test_batch_report_revoke_then_reject_scenario(self) -> None:
        self.register_orders()
        self.import_rows("order_id,total\n1,10\n2,20\n")
        self.write_csv("order_id,total\n3,\n")
        self.assertEqual(self.invoke("run-import", "orders").returncode, 1)
        self.assertEqual(self.invoke("revoke-batch", "orders", "1").returncode, 0)
        self.digest_before = self.db_digest()
        result = self.invoke("batch-report", "orders", "1", "2")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(
            result.stdout.splitlines(),
            [
                "ok:0:0:0",
                "---",
                "rejected:1:0:1",
                "2\t0\t1",
                "---",
                "revoked:1:2:0",
                "1\t2\t0",
            ],
        )
        self.assert_readonly(result)

    def test_batch_report_all_three_sections_with_details(self) -> None:
        self.register_orders()
        self.import_rows("order_id,total\n1,10\n2,20\n")
        self.write_csv("order_id,total\n3,\n4,\n")
        self.assertEqual(self.invoke("run-import", "orders").returncode, 1)
        self.import_rows("order_id,total\n5,50\n6,60\n7,70\n")
        self.assertEqual(self.invoke("revoke-batch", "orders", "1").returncode, 0)
        result = self.invoke("batch-report", "orders", "1", "3")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(
            result.stdout.splitlines(),
            [
                "ok:1:3:0",
                "3\t3\t0",
                "---",
                "rejected:1:0:2",
                "2\t0\t2",
                "---",
                "revoked:1:2:0",
                "1\t2\t0",
            ],
        )

    def test_batch_report_sections_stay_when_range_is_empty(self) -> None:
        self.register_orders()
        self.import_rows("order_id,total\n1,10\n")
        self.digest_before = self.db_digest()
        for arguments in [("orders", "2", "3"), ("orders", "9", "100")]:
            with self.subTest(arguments=arguments):
                result = self.invoke("batch-report", *arguments)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(
                    result.stdout.splitlines(),
                    ["ok:0:0:0", "---", "rejected:0:0:0", "---", "revoked:0:0:0"],
                )
                self.assertEqual(self.db_digest(), self.digest_before)

    def test_batch_report_matches_batch_stats_and_detail(self) -> None:
        self.register_orders()
        self.import_rows("order_id,total\n1,10\n2,20\n")
        self.import_rows("order_id,total\n3,30\n4,40\n")
        self.write_csv("order_id,total\n5,\n6,\n")
        self.assertEqual(self.invoke("run-import", "orders").returncode, 1)
        self.import_rows("order_id,total\n7,70\n")
        self.assertEqual(self.invoke("revoke-batch", "orders", "1").returncode, 0)
        report = self.invoke("batch-report", "orders", "1", "4")
        self.assertEqual(report.returncode, 0, report.stderr)
        blocks = report.stdout.split("\n---\n")
        self.assertEqual(len(blocks), 3)
        stats = self.invoke("batch-stats", "orders", "1", "4")
        self.assertEqual(stats.returncode, 0, stats.stderr)
        total, ok_count, rejected_count, revoked_count, succeeded_sum, quarantined_sum = (
            int(value) for value in stats.stdout.split("\t")
        )
        counts = {"ok": ok_count, "rejected": rejected_count, "revoked": revoked_count}
        detail_rows = 0
        succeeded_headers = quarantined_headers = 0
        succeeded_rows = quarantined_rows = 0
        for status, block in zip(("ok", "rejected", "revoked"), blocks):
            lines = block.splitlines()
            header, detail_lines = lines[0], lines[1:]
            label, count_text, succeeded_text, quarantined_text = header.split(":")
            self.assertEqual(label, status)
            self.assertEqual(int(count_text), counts[status])
            self.assertEqual(len(detail_lines), counts[status])
            detail = self.invoke(
                "batch-stats-detail", "orders", "1", "4", status
            )
            self.assertEqual(detail.returncode, 0, detail.stderr)
            self.assertEqual("\n".join(detail_lines), detail.stdout.rstrip("\n"))
            detail_rows += len(detail_lines)
            succeeded_headers += int(succeeded_text)
            quarantined_headers += int(quarantined_text)
            for line in detail_lines:
                succeeded, quarantined = (
                    int(value) for value in line.split("\t")[1:]
                )
                succeeded_rows += succeeded
                quarantined_rows += quarantined
        self.assertEqual(detail_rows, total)
        self.assertEqual(succeeded_headers, succeeded_rows)
        self.assertEqual(quarantined_headers, quarantined_rows)
        self.assertEqual(succeeded_headers, succeeded_sum)
        self.assertEqual(quarantined_headers, quarantined_sum)

    def test_batch_report_range_bounds_are_inclusive(self) -> None:
        self.register_orders()
        self.import_rows("order_id,total\n1,10\n")
        self.import_rows("order_id,total\n2,20\n3,30\n")
        self.import_rows("order_id,total\n4,40\n")
        result = self.invoke("batch-report", "orders", "2", "2")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(
            result.stdout.splitlines(),
            ["ok:1:2:0", "2\t2\t0", "---", "rejected:0:0:0", "---", "revoked:0:0:0"],
        )
        result = self.invoke("batch-report", "orders", "2", "9")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(
            result.stdout.splitlines(),
            [
                "ok:2:3:0",
                "2\t2\t0",
                "3\t1\t0",
                "---",
                "rejected:0:0:0",
                "---",
                "revoked:0:0:0",
            ],
        )

    def test_batch_report_does_not_change_other_query_results(self) -> None:
        self.register_orders()
        self.import_rows("order_id,total\n1,10\n2,20\n")
        self.write_csv("order_id,total\n3,\n")
        self.assertEqual(self.invoke("run-import", "orders").returncode, 1)
        self.assertEqual(self.invoke("revoke-batch", "orders", "1").returncode, 0)
        commands = [
            ("show-batch", "orders", "1"),
            ("show-rows", "orders", "1"),
            ("find-dupes", "orders", "amount"),
            ("batch-stats", "orders", "1", "2"),
            ("batch-stats-detail", "orders", "1", "2", "revoked"),
        ]
        before = {command: self.invoke(*command) for command in commands}
        report = self.invoke("batch-report", "orders", "1", "2")
        self.assertEqual(report.returncode, 0, report.stderr)
        for command in commands:
            after = self.invoke(*command)
            self.assertEqual(
                (after.returncode, after.stdout, after.stderr),
                (
                    before[command].returncode,
                    before[command].stdout,
                    before[command].stderr,
                ),
                command,
            )

    def test_batch_report_rejects_invalid_arguments(self) -> None:
        self.register_orders()
        self.import_rows("order_id,total\n1,10\n")
        self.digest_before = self.db_digest()
        invalid = [
            ("missing", "1", "2"),
            ("TRUE", "1", "2"),
            ("  ", "1", "2"),
            ("orders", "0", "2"),
            ("orders", "-1", "2"),
            ("orders", "1", "0"),
            ("orders", "2", "-3"),
            ("orders", "abc", "2"),
            ("orders", "1", "2.5"),
            ("orders", "2", "1"),
        ]
        for arguments in invalid:
            with self.subTest(arguments=arguments):
                result = self.invoke("batch-report", *arguments)
                self.assertEqual(result.returncode, 1)
                self.assertTrue(result.stderr.startswith("Error:"))
                self.assertEqual(len(result.stderr.strip().splitlines()), 1)
                self.assertEqual(result.stdout, "")
                self.assertEqual(self.db_digest(), self.digest_before)


class BatchReconcileTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.db_path = Path(self.tmp.name) / "import_ledger.db"
        self.csv_path = Path(self.tmp.name) / "orders.csv"

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

    def write_csv(self, text: str) -> None:
        self.csv_path.write_text(text, encoding="utf-8")

    def db_digest(self) -> str:
        return hashlib.sha256(self.db_path.read_bytes()).hexdigest()

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

    def import_rows(self, rows: str) -> None:
        self.write_csv(rows)
        result = self.invoke("run-import", "orders")
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_consistent_after_import_reject_revoke_scenario(self) -> None:
        self.register_orders()
        self.import_rows("order_id,total\n1,10\n2,20\n")
        self.write_csv("order_id,total\n3,\n")
        self.assertEqual(self.invoke("run-import", "orders").returncode, 1)
        self.assertEqual(self.invoke("revoke-batch", "orders", "1").returncode, 0)
        digest_before = self.db_digest()
        result = self.invoke("batch-reconcile", "orders", "1", "2")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(
            result.stdout.splitlines(),
            ["Result: batch-reconcile orders 1 2 consistent"],
        )
        self.assertEqual(result.stderr, "")
        self.assertEqual(self.db_digest(), digest_before)

    def test_consistent_with_all_three_statuses_and_empty_sections(self) -> None:
        self.register_orders()
        self.import_rows("order_id,total\n1,10\n2,20\n")
        self.write_csv("order_id,total\n3,\n4,\n")
        self.assertEqual(self.invoke("run-import", "orders").returncode, 1)
        self.import_rows("order_id,total\n5,50\n6,60\n7,70\n")
        self.assertEqual(self.invoke("revoke-batch", "orders", "1").returncode, 0)
        digest_before = self.db_digest()
        for arguments in [("orders", "1", "3"), ("orders", "4", "9")]:
            with self.subTest(arguments=arguments):
                result = self.invoke("batch-reconcile", *arguments)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(
                    result.stdout,
                    f"Result: batch-reconcile {arguments[0]} {arguments[1]}"
                    f" {arguments[2]} consistent\n",
                )
                self.assertEqual(self.db_digest(), digest_before)

    def test_field_order_and_normalized_detail_text(self) -> None:
        self.assertEqual(
            ledger._RECONCILE_FIELDS,
            (
                "total_batches",
                "ok_batches",
                "rejected_batches",
                "revoked_batches",
                "succeeded_rows",
                "quarantined_rows",
                "ok_detail",
                "rejected_detail",
                "revoked_detail",
            ),
        )
        self.assertEqual(
            ledger._format_reconcile_detail([(1, 3, 0), (3, 2, 1)]),
            "1,3,0;3,2,1",
        )
        self.assertEqual(ledger._format_reconcile_detail([]), "")

    def test_mismatch_lines_rendered_in_field_order_with_exit_code_1(self) -> None:
        # 两路实际同出一份快照，正常数据不会产生差异；这里直接构造差异列表，
        # 校验 CLI 的逐行格式、字段顺序与退出码。
        self.register_orders()
        self.import_rows("order_id,total\n1,10\n")
        digest_before = self.db_digest()
        fabricated = [
            ("total_batches", "3", "2"),
            ("ok_detail", "1,2,0;3,1,0", "1,2,0"),
            ("revoked_detail", "", "4,1,0"),
        ]
        with mock.patch.object(
            ledger, "batch_reconcile", return_value=fabricated
        ), mock.patch.dict(os.environ, {"IMPORT_LEDGER_DB": str(self.db_path)}):
            stdout = io.StringIO()
            stderr = io.StringIO()
            with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                exit_code = cli.main(["batch-reconcile", "orders", "1", "4"])
        self.assertEqual(exit_code, 1)
        self.assertEqual(stderr.getvalue(), "")
        self.assertEqual(
            stdout.getvalue().splitlines(),
            [
                "Mismatch: total_batches batch-report=3 other=2",
                "Mismatch: ok_detail batch-report=1,2,0;3,1,0 other=1,2,0",
                "Mismatch: revoked_detail batch-report= other=4,1,0",
            ],
        )
        self.assertEqual(self.db_digest(), digest_before)

    def test_rejects_invalid_arguments_without_touching_database(self) -> None:
        self.register_orders()
        self.import_rows("order_id,total\n1,10\n")
        digest_before = self.db_digest()
        invalid = [
            ("missing", "1", "2"),
            ("TRUE", "1", "2"),
            ("  ", "1", "2"),
            ("orders", "0", "2"),
            ("orders", "-1", "2"),
            ("orders", "1", "0"),
            ("orders", "2", "-3"),
            ("orders", "abc", "2"),
            ("orders", "1", "2.5"),
            ("orders", "2", "1"),
        ]
        for arguments in invalid:
            with self.subTest(arguments=arguments):
                result = self.invoke("batch-reconcile", *arguments)
                self.assertEqual(result.returncode, 1)
                self.assertTrue(result.stderr.startswith("Error:"))
                self.assertEqual(len(result.stderr.strip().splitlines()), 1)
                self.assertEqual(result.stdout, "")
                self.assertEqual(self.db_digest(), digest_before)

    def test_does_not_change_other_query_results(self) -> None:
        self.register_orders()
        self.import_rows("order_id,total\n1,10\n2,20\n")
        self.write_csv("order_id,total\n3,\n")
        self.assertEqual(self.invoke("run-import", "orders").returncode, 1)
        self.assertEqual(self.invoke("revoke-batch", "orders", "1").returncode, 0)
        commands = [
            ("show-batch", "orders", "1"),
            ("show-rows", "orders", "1"),
            ("find-dupes", "orders", "amount"),
            ("batch-stats", "orders", "1", "2"),
            ("batch-stats-detail", "orders", "1", "2", "revoked"),
            ("batch-report", "orders", "1", "2"),
        ]
        before = {command: self.invoke(*command) for command in commands}
        reconcile = self.invoke("batch-reconcile", "orders", "1", "2")
        self.assertEqual(reconcile.returncode, 0, reconcile.stderr)
        for command in commands:
            after = self.invoke(*command)
            self.assertEqual(
                (after.returncode, after.stdout, after.stderr),
                (
                    before[command].returncode,
                    before[command].stdout,
                    before[command].stderr,
                ),
                command,
            )


if __name__ == "__main__":
    unittest.main()
