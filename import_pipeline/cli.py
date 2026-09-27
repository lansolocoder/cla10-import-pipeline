"""Command-line entry point."""

import argparse
import sys
from collections.abc import Sequence

from . import __version__
from . import ledger


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="import-pipeline",
        description="Local 数据导入与校验台账 ledger.",
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    subparsers = parser.add_subparsers(dest="command", metavar="command")

    add_source = subparsers.add_parser(
        "add-source", help="注册来源配置: 来源名 CSV路径 必需字段..."
    )
    add_source.add_argument("name", help="来源名（非空、唯一）")
    add_source.add_argument("csv_path", help="CSV 文件路径")
    add_source.add_argument("fields", nargs="+", help="必需字段名（至少一个，不重复）")

    add_mapping = subparsers.add_parser(
        "add-mapping", help="注册字段映射: 来源名 源列名 目标列名"
    )
    add_mapping.add_argument("source", help="已注册的来源名")
    add_mapping.add_argument("source_column", help="源列名（同一来源下唯一）")
    add_mapping.add_argument("target_column", help="目标列名")

    subparsers.add_parser("list-sources", help="列出全部来源配置")

    list_mappings = subparsers.add_parser(
        "list-mappings", help="列出某来源的字段映射"
    )
    list_mappings.add_argument("source", help="已注册的来源名")

    run_import = subparsers.add_parser(
        "run-import", help="执行一次导入: 来源名"
    )
    run_import.add_argument("source", help="已注册的来源名")

    show_batch = subparsers.add_parser(
        "show-batch", help="查询批次记录: 来源名 批次号"
    )
    show_batch.add_argument("source", help="已注册的来源名")
    show_batch.add_argument("batch_no", type=int, help="起始批次号（含）")

    revoke_batch = subparsers.add_parser(
        "revoke-batch", help="撤销单个批次: 来源名 批次号"
    )
    revoke_batch.add_argument("source", help="已注册的来源名")
    revoke_batch.add_argument("batch_no", type=int, help="待撤销批次号（正整数）")

    show_rows = subparsers.add_parser(
        "show-rows", help="列出批次已导入行: 来源名 批次号"
    )
    show_rows.add_argument("source", help="已注册的来源名")
    show_rows.add_argument("batch_no", type=int, help="批次号（正整数）")

    find_dupes = subparsers.add_parser(
        "find-dupes", help="按目标列值查找重复行: 来源名 目标列名"
    )
    find_dupes.add_argument("source", help="已注册的来源名")
    find_dupes.add_argument("target_column", help="目标列名")

    batch_stats = subparsers.add_parser(
        "batch-stats", help="跨批次状态汇总: 来源名 起始批次号 结束批次号"
    )
    batch_stats.add_argument("source", help="已注册的来源名")
    batch_stats.add_argument("start_batch", help="起始批次号（含，正整数）")
    batch_stats.add_argument("end_batch", help="结束批次号（含，正整数）")

    batch_report = subparsers.add_parser(
        "batch-report",
        help="一次只读扫描给出区间内 ok/rejected/revoked 三段汇总与明细:"
        " 来源名 起始批次号 结束批次号",
    )
    batch_report.add_argument("source", help="已注册的来源名")
    batch_report.add_argument("start_batch", help="起始批次号（含，正整数）")
    batch_report.add_argument("end_batch", help="结束批次号（含，正整数）")

    batch_stats_detail = subparsers.add_parser(
        "batch-stats-detail",
        help="区间内指定状态批次明细: 来源名 起始批次号 结束批次号 状态",
    )
    batch_stats_detail.add_argument("source", help="已注册的来源名")
    batch_stats_detail.add_argument("start_batch", help="起始批次号（含，正整数）")
    batch_stats_detail.add_argument("end_batch", help="结束批次号（含，正整数）")
    batch_stats_detail.add_argument(
        "status", help="批次状态（只接受小写 ok、rejected、revoked）"
    )

    batch_reconcile = subparsers.add_parser(
        "batch-reconcile",
        help="对账 batch-report 与 batch-stats/batch-stats-detail 两路口径:"
        " 来源名 起始批次号 结束批次号",
    )
    batch_reconcile.add_argument("source", help="已注册的来源名")
    batch_reconcile.add_argument("start_batch", help="起始批次号（含，正整数）")
    batch_reconcile.add_argument("end_batch", help="结束批次号（含，正整数）")

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command is None:
        parser.print_help()
        return 0

    readonly = args.command in (
        "show-rows",
        "find-dupes",
        "batch-stats",
        "batch-stats-detail",
        "batch-report",
        "batch-reconcile",
    )
    conn = ledger.connect_readonly() if readonly else ledger.connect()
    try:
        if args.command == "add-source":
            count = ledger.add_source(conn, args.name, args.csv_path, args.fields)
            print(f"Result: add-source {args.name} {count}")
        elif args.command == "add-mapping":
            count = ledger.add_mapping(
                conn, args.source, args.source_column, args.target_column
            )
            print(f"Result: add-mapping {args.source} {count}")
        elif args.command == "list-sources":
            for name, csv_path, fields in ledger.list_sources(conn):
                print(f"{name}\t{csv_path}\t{','.join(fields)}")
        elif args.command == "list-mappings":
            for source_column, target_column in ledger.list_mappings(conn, args.source):
                print(f"{source_column}\t{target_column}")
        elif args.command == "run-import":
            count = ledger.run_import(conn, args.source)
            print(f"Result: run-import {args.source} {count}")
        elif args.command == "revoke-batch":
            ledger.revoke_batch(conn, args.source, args.batch_no)
            print(f"Result: revoke-batch {args.source} {args.batch_no}")
        elif args.command == "show-batch":
            for batch_no, status, succeeded, quarantined in ledger.show_batches(
                conn, args.source, args.batch_no
            ):
                print(f"{batch_no}\t{status}\t{succeeded}\t{quarantined}")
        elif args.command == "show-rows":
            for line in ledger.show_rows(conn, args.source, args.batch_no):
                print(line)
        elif args.command == "find-dupes":
            for value, count, batch_numbers in ledger.find_dupes(
                conn, args.source, args.target_column
            ):
                batches = ",".join(str(batch_no) for batch_no in batch_numbers)
                print(f"{value}\t{count}\t{batches}")
        elif args.command == "batch-stats":
            stats = ledger.batch_stats(
                conn, args.source, args.start_batch, args.end_batch
            )
            print("\t".join(str(value) for value in stats))
        elif args.command == "batch-report":
            sections = ledger.batch_report(
                conn, args.source, args.start_batch, args.end_batch
            )
            blocks = []
            for status, batch_count, succeeded_total, quarantined_total, details in sections:
                lines = [
                    f"{status}:{batch_count}:{succeeded_total}:{quarantined_total}"
                ]
                lines.extend(
                    f"{batch_no}\t{succeeded}\t{quarantined}"
                    for batch_no, succeeded, quarantined in details
                )
                blocks.append("\n".join(lines))
            print("\n---\n".join(blocks))
        elif args.command == "batch-stats-detail":
            for batch_no, succeeded, quarantined in ledger.batch_stats_detail(
                conn, args.source, args.start_batch, args.end_batch, args.status
            ):
                print(f"{batch_no}\t{succeeded}\t{quarantined}")
        elif args.command == "batch-reconcile":
            mismatches = ledger.batch_reconcile(
                conn, args.source, args.start_batch, args.end_batch
            )
            if mismatches:
                for field, report_value, other_value in mismatches:
                    print(
                        f"Mismatch: {field} batch-report={report_value}"
                        f" other={other_value}"
                    )
                return 1
            print(
                f"Result: batch-reconcile {args.source}"
                f" {args.start_batch} {args.end_batch} consistent"
            )
    except ledger.LedgerError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1
    finally:
        conn.close()
    return 0
