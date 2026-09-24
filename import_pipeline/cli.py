"""Command-line entry point."""

import argparse
import sys
from collections.abc import Sequence

from . import __version__, store


def _add_source(args: argparse.Namespace) -> int:
    store.add_source(name=args.name, path=args.path, fields=args.fields)
    print(f"Result: add-source {args.name} 1")
    return 0


def _add_mapping(args: argparse.Namespace) -> int:
    store.add_mapping(
        source_name=args.source,
        source_column=args.source_column,
        target_column=args.target_column,
    )
    print(f"Result: add-mapping {args.source} 1")
    return 0


def _list_sources(_args: argparse.Namespace) -> int:
    for source in store.list_sources():
        print(f"{source.name}\t{source.path}\t{','.join(source.fields)}")
    return 0


def _list_mappings(args: argparse.Namespace) -> int:
    for mapping in store.list_mappings(args.source):
        print(f"{mapping.source_column}\t{mapping.target_column}")
    return 0


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="import-pipeline",
        description="Local 数据导入与校验台账 ledger.",
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")

    subparsers = parser.add_subparsers(dest="command")

    p_add_source = subparsers.add_parser(
        "add-source",
        aliases=("register-source",),
        help="注册来源配置（来源名、CSV 路径、必需字段名列表）",
        description="注册来源配置：来源名为唯一业务键，字段名至少一个且不可重复。",
    )
    p_add_source.add_argument("--name", required=True, help="来源名（非空，唯一）")
    p_add_source.add_argument("--path", required=True, help="CSV 文件路径（非空）")
    p_add_source.add_argument(
        "--field",
        dest="fields",
        required=True,
        action="append",
        metavar="FIELD",
        help="必需字段名，可重复传入，至少一个；名称非空且不重复（按传入顺序登记）",
    )
    p_add_source.set_defaults(handler=_add_source)

    p_add_mapping = subparsers.add_parser(
        "add-mapping",
        aliases=("register-mapping",),
        help="注册字段映射（来源名、源列名、目标列名）",
        description="注册字段映射：同一来源下同一源列名仅保留首次登记的映射。",
    )
    p_add_mapping.add_argument("--source", required=True, help="已注册的来源名")
    p_add_mapping.add_argument("--source-column", required=True, help="源列名（非空）")
    p_add_mapping.add_argument("--target-column", required=True, help="目标列名（非空）")
    p_add_mapping.set_defaults(handler=_add_mapping)

    p_list_sources = subparsers.add_parser(
        "list-sources",
        help="列出全部来源配置（制表符分隔，字段名以逗号连接）",
    )
    p_list_sources.set_defaults(handler=_list_sources)

    p_list_mappings = subparsers.add_parser(
        "list-mappings",
        help="列出某来源的字段映射（制表符分隔，按注册顺序）",
    )
    p_list_mappings.add_argument("--source", required=True, help="已注册的来源名")
    p_list_mappings.set_defaults(handler=_list_mappings)

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    handler = getattr(args, "handler", None)
    if handler is None:
        parser.print_help()
        return 0
    try:
        return handler(args)
    except store.ConfigError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
