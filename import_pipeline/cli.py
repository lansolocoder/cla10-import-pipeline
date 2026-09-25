"""Command-line entry point."""

import argparse
import re
import sys
from collections.abc import Sequence

from . import __version__
from . import importer
from . import ledger


def _batch_no(value: str) -> int:
    """批次号必须是十进制正整数，否则按用法错误（退出码 2）处理。"""
    if not re.fullmatch(r"[0-9]+", value) or int(value) < 1:
        raise argparse.ArgumentTypeError(f"批次号必须是十进制正整数: {value}")
    return int(value)


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

    import_cmd = subparsers.add_parser(
        "import", help="执行批次导入: 来源名"
    )
    import_cmd.add_argument("source", help="已注册的来源名")

    batches_cmd = subparsers.add_parser(
        "batches", help="列出某来源的批次台账: 来源名"
    )
    batches_cmd.add_argument("source", help="已注册的来源名")

    reimport_cmd = subparsers.add_parser(
        "reimport", help="重跑指定批次并整体替换其记录: 来源名 批次号"
    )
    reimport_cmd.add_argument("source", help="已注册的来源名")
    reimport_cmd.add_argument("batch_no", type=_batch_no, help="十进制正整数批次号")

    rollback_cmd = subparsers.add_parser(
        "rollback", help="撤销指定批次: 来源名 批次号"
    )
    rollback_cmd.add_argument("source", help="已注册的来源名")
    rollback_cmd.add_argument("batch_no", type=_batch_no, help="十进制正整数批次号")

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command is None:
        parser.print_help()
        return 0

    conn = ledger.connect()
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
        elif args.command == "import":
            result = importer.import_source(conn, args.source)
            print(
                f"Result: import {result.source} {result.batch_no}"
                f" {result.total_rows} {result.imported_rows}"
                f" {result.duplicate_rows} {result.rejected_rows}"
                f" {result.incremental_rows}"
            )
        elif args.command == "batches":
            rows = ledger.list_batches(conn, args.source)
            for batch_no, total, imported, duplicates, rejected, incremental in rows:
                print(
                    f"{batch_no}\t{total}\t{imported}"
                    f"\t{duplicates}\t{rejected}\t{incremental}"
                )
            if rows:
                print(f"Result: batches {args.source}")
        elif args.command == "reimport":
            result = importer.reimport_source(conn, args.source, args.batch_no)
            print(
                f"Result: reimport {result.source} {result.batch_no}"
                f" {result.total_rows} {result.imported_rows}"
                f" {result.duplicate_rows} {result.rejected_rows}"
                f" {result.incremental_rows}"
            )
        elif args.command == "rollback":
            total = importer.rollback_batch(conn, args.source, args.batch_no)
            print(f"Result: rollback {args.source} {args.batch_no} {total}")
    except ledger.LedgerError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1
    finally:
        conn.close()
    return 0
