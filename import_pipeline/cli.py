"""Command-line entry point."""

import argparse
import re
import sys
from collections.abc import Sequence

from . import __version__
from . import importer
from . import ledger

_POSITIVE_INT = re.compile(r"[1-9][0-9]*")


def positive_int(value: str) -> int:
    """十进制正整数（不接受前导零、符号或其他写法），非法时按用法错误处理。"""
    if not _POSITIVE_INT.fullmatch(value):
        raise argparse.ArgumentTypeError(f"批次号必须是十进制正整数: {value}")
    return int(value)


def _rule_spec(
    rule_type: str, min_value: str | None, max_value: str | None, candidates: str | None
) -> str:
    """rules 输出的第三列：decimal 为 min~max（缺省侧留空），enum 为候选值，其余为 -。"""
    if rule_type == "enum":
        return candidates or ""
    if rule_type == "decimal" and (min_value is not None or max_value is not None):
        return f"{min_value or ''}~{max_value or ''}"
    return "-"


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

    add_rule = subparsers.add_parser(
        "add-rule", help="注册校验规则: 来源名 字段名 类型 [边界或候选值]"
    )
    add_rule.add_argument("source", help="已注册的来源名")
    add_rule.add_argument("field", help="该来源已声明的必需字段名")
    add_rule.add_argument("rule_type", help="规则类型: decimal / date / enum")
    add_rule.add_argument(
        "params",
        nargs="*",
        help="decimal 为 min~max 边界（可省略或留空一侧），enum 为逗号分隔候选值",
    )

    rules_cmd = subparsers.add_parser(
        "rules", help="列出某来源的全部校验规则（只读）"
    )
    rules_cmd.add_argument("source", help="已注册的来源名")

    list_mappings = subparsers.add_parser(
        "list-mappings", help="列出某来源的字段映射"
    )
    list_mappings.add_argument("source", help="已注册的来源名")

    import_cmd = subparsers.add_parser(
        "import", help="执行批次导入: 来源名"
    )
    import_cmd.add_argument("source", help="已注册的来源名")

    batches = subparsers.add_parser(
        "batches", help="查看某来源的全部批次计数（只读）"
    )
    batches.add_argument("source", help="已注册的来源名")

    reimport_cmd = subparsers.add_parser(
        "reimport", help="重跑指定批次并原子替换原批次记录"
    )
    reimport_cmd.add_argument("source", help="已注册的来源名")
    reimport_cmd.add_argument("batch_no", type=positive_int, help="批次号（十进制正整数）")

    rollback_cmd = subparsers.add_parser(
        "rollback", help="撤销指定批次：删除批次记录及其新增签名"
    )
    rollback_cmd.add_argument("source", help="已注册的来源名")
    rollback_cmd.add_argument("batch_no", type=positive_int, help="批次号（十进制正整数）")

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
        elif args.command == "add-rule":
            count = ledger.add_rule(
                conn, args.source, args.field, args.rule_type, args.params
            )
            print(
                f"Result: add-rule {args.source} {args.field}"
                f" {args.rule_type} {count}"
            )
        elif args.command == "rules":
            for field, rule_type, lo, hi, candidates in ledger.list_rules(
                conn, args.source
            ):
                print(f"{field}\t{rule_type}\t{_rule_spec(rule_type, lo, hi, candidates)}")
            print(f"Result: rules {args.source}")
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
            for (
                batch_no,
                total_rows,
                imported_rows,
                duplicate_rows,
                rejected_rows,
                incremental_rows,
            ) in ledger.list_batches(conn, args.source):
                print(
                    f"{batch_no}\t{total_rows}\t{imported_rows}"
                    f"\t{duplicate_rows}\t{rejected_rows}\t{incremental_rows}"
                )
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
            total_rows = ledger.rollback_batch(conn, args.source, args.batch_no)
            print(
                f"Result: rollback {args.source} {args.batch_no} {total_rows}"
            )
    except ledger.LedgerError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1
    finally:
        conn.close()
    return 0
