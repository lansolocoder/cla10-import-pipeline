"""SQLite 持久化：来源配置与字段映射台账。

仓库根目录下的 import_ledger.db 是唯一持久化载体，仅使用 sqlite3 标准库。
"""

from __future__ import annotations

import os
import re
import sqlite3
from collections.abc import Sequence
from datetime import date
from decimal import Decimal
from pathlib import Path

DEFAULT_DB_PATH = Path(__file__).resolve().parent.parent / "import_ledger.db"

# 布尔与状态字面值只接受小写形式；大小写不同的取值视为非法输入。
RESERVED_LITERALS = frozenset({"true", "false", "ok", "failed", "rejected"})

_SCHEMA = """
CREATE TABLE IF NOT EXISTS sources (
    name TEXT PRIMARY KEY,
    csv_path TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS source_fields (
    source_name TEXT NOT NULL REFERENCES sources (name),
    field_name TEXT NOT NULL,
    PRIMARY KEY (source_name, field_name)
);
CREATE TABLE IF NOT EXISTS field_mappings (
    source_name TEXT NOT NULL REFERENCES sources (name),
    source_column TEXT NOT NULL,
    target_column TEXT NOT NULL,
    PRIMARY KEY (source_name, source_column)
);
CREATE TABLE IF NOT EXISTS import_batches (
    source_name TEXT NOT NULL REFERENCES sources (name),
    batch_no INTEGER NOT NULL,
    total_rows INTEGER NOT NULL,
    imported_rows INTEGER NOT NULL,
    duplicate_rows INTEGER NOT NULL,
    rejected_rows INTEGER NOT NULL,
    incremental_rows INTEGER NOT NULL,
    PRIMARY KEY (source_name, batch_no)
);
CREATE TABLE IF NOT EXISTS imported_records (
    source_name TEXT NOT NULL REFERENCES sources (name),
    signature TEXT NOT NULL,
    batch_no INTEGER NOT NULL,
    PRIMARY KEY (source_name, signature)
);
CREATE TABLE IF NOT EXISTS field_rules (
    source_name TEXT NOT NULL REFERENCES sources (name),
    field_name TEXT NOT NULL,
    rule_type TEXT NOT NULL,
    min_value TEXT,
    max_value TEXT,
    candidates TEXT,
    PRIMARY KEY (source_name, field_name, rule_type)
);
"""


class LedgerError(Exception):
    """业务规则拒绝：信息写 stderr，退出码为 1（非 argparse 的 2）。"""


def db_path() -> Path:
    override = os.environ.get("IMPORT_LEDGER_DB")
    if override:
        return Path(override)
    return DEFAULT_DB_PATH


def connect() -> sqlite3.Connection:
    conn = sqlite3.connect(db_path())
    conn.execute("PRAGMA foreign_keys = ON")
    conn.executescript(_SCHEMA)
    return conn


def _validate_token(value: str, label: str) -> None:
    if not value or not value.strip():
        raise LedgerError(f"{label}不能为空")
    if value.lower() in RESERVED_LITERALS and value not in RESERVED_LITERALS:
        raise LedgerError(f"{label}含非法字面值: {value}")


def add_source(
    conn: sqlite3.Connection, name: str, csv_path: str, fields: Sequence[str]
) -> int:
    """注册来源配置，返回登记的字段数。失败时不落库。"""
    _validate_token(name, "来源名")
    _validate_token(csv_path, "文件路径")
    if not fields:
        raise LedgerError("必需字段列表至少包含一个字段名")
    seen: set[str] = set()
    for field in fields:
        _validate_token(field, "字段名")
        if field in seen:
            raise LedgerError(f"字段名重复: {field}")
        seen.add(field)
    with conn:  # 单次操作原子提交，异常即回滚
        if conn.execute(
            "SELECT 1 FROM sources WHERE name = ?", (name,)
        ).fetchone():
            raise LedgerError(f"来源已存在: {name}")
        conn.execute(
            "INSERT INTO sources (name, csv_path) VALUES (?, ?)", (name, csv_path)
        )
        conn.executemany(
            "INSERT INTO source_fields (source_name, field_name) VALUES (?, ?)",
            [(name, field) for field in fields],
        )
    return len(fields)


def add_mapping(
    conn: sqlite3.Connection, source: str, source_column: str, target_column: str
) -> int:
    """注册字段映射，返回 1。冲突或来源不存在时拒绝并保留原状。"""
    _validate_token(source, "来源名")
    _validate_token(source_column, "源列名")
    _validate_token(target_column, "目标列名")
    with conn:
        if not conn.execute(
            "SELECT 1 FROM sources WHERE name = ?", (source,)
        ).fetchone():
            raise LedgerError(f"来源不存在: {source}")
        if conn.execute(
            "SELECT 1 FROM field_mappings WHERE source_name = ? AND source_column = ?",
            (source, source_column),
        ).fetchone():
            raise LedgerError(f"源列名映射冲突: {source_column}")
        conn.execute(
            "INSERT INTO field_mappings (source_name, source_column, target_column)"
            " VALUES (?, ?, ?)",
            (source, source_column, target_column),
        )
    return 1


def list_sources(conn: sqlite3.Connection) -> list[tuple[str, str, list[str]]]:
    """按注册顺序返回 (来源名, 文件路径, 字段名列表)。"""
    rows = conn.execute("SELECT name, csv_path FROM sources ORDER BY rowid").fetchall()
    result = []
    for name, csv_path in rows:
        fields = [
            row[0]
            for row in conn.execute(
                "SELECT field_name FROM source_fields WHERE source_name = ?"
                " ORDER BY rowid",
                (name,),
            )
        ]
        result.append((name, csv_path, fields))
    return result


def list_mappings(conn: sqlite3.Connection, source: str) -> list[tuple[str, str]]:
    """按注册顺序返回某来源的 (源列名, 目标列名)；来源不存在时拒绝。"""
    if not conn.execute(
        "SELECT 1 FROM sources WHERE name = ?", (source,)
    ).fetchone():
        raise LedgerError(f"来源不存在: {source}")
    return conn.execute(
        "SELECT source_column, target_column FROM field_mappings"
        " WHERE source_name = ? ORDER BY rowid",
        (source,),
    ).fetchall()


# 校验规则类型；类型字面值只接受小写形式，其他写法一律拒绝。
RULE_TYPES = ("decimal", "date", "enum")

# 十进制数值：可选正负号，数字至多一个小数点，不允许空格或其他符号。
_DECIMAL_VALUE = re.compile(r"[+-]?([0-9]+(\.[0-9]*)?|\.[0-9]+)")
# ISO 日期：严格的 YYYY-MM-DD 形式，是否真实存在另行校验。
_ISO_DATE = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}")


def is_decimal_value(value: str) -> bool:
    """判断是否为十进制数值写法（空格、多余小数点、其他符号均非法）。"""
    return _DECIMAL_VALUE.fullmatch(value) is not None


def is_iso_date(value: str) -> bool:
    """判断是否为格式合法且真实存在的 YYYY-MM-DD 日期。"""
    if _ISO_DATE.fullmatch(value) is None:
        return False
    try:
        date(int(value[0:4]), int(value[5:7]), int(value[8:10]))
    except ValueError:
        return False
    return True


def _parse_decimal_bounds(spec: str) -> tuple[str | None, str | None]:
    """解析 `min~max` 形式的边界，任一侧可留空；非法时抛 LedgerError。"""
    parts = spec.split("~")
    if len(parts) != 2:
        raise LedgerError(f"边界格式须为 min~max: {spec}")
    low, high = parts
    if not low and not high:
        raise LedgerError("decimal 规则未指定边界")
    for bound in (low, high):
        if bound and not is_decimal_value(bound):
            raise LedgerError(f"边界非数值: {bound}")
    if low and high and Decimal(low) > Decimal(high):
        raise LedgerError(f"最小值大于最大值: {low}~{high}")
    return low or None, high or None


def _parse_candidates(spec: str) -> str:
    """校验逗号分隔的候选值列表（非空、无空项、不重复），原样返回。"""
    items = spec.split(",")
    if any(item == "" for item in items):
        raise LedgerError(f"候选值含空项: {spec}")
    if len(set(items)) != len(items):
        raise LedgerError(f"候选值重复: {spec}")
    return spec


def add_rule(
    conn: sqlite3.Connection,
    source: str,
    field: str,
    rule_type: str,
    spec: str | None = None,
) -> int:
    """注册校验规则，返回 1。校验失败或冲突时拒绝且已有规则保持原样。

    spec 为 decimal 的 `min~max` 边界（可省略表示无边界）或 enum 的
    逗号分隔候选值列表（必填）；date 不接受 spec。
    """
    _validate_token(source, "来源名")
    _validate_token(field, "字段名")
    if rule_type not in RULE_TYPES:
        raise LedgerError(f"未知规则类型: {rule_type}")
    min_value: str | None = None
    max_value: str | None = None
    candidates: str | None = None
    if rule_type == "decimal":
        if spec is not None:
            min_value, max_value = _parse_decimal_bounds(spec)
    elif rule_type == "date":
        if spec is not None:
            raise LedgerError("date 规则不接受边界或候选值参数")
    else:  # enum
        if spec is None:
            raise LedgerError("enum 规则未指定候选值")
        candidates = _parse_candidates(spec)
    with conn:  # 单次操作原子提交，异常即回滚
        if not conn.execute(
            "SELECT 1 FROM sources WHERE name = ?", (source,)
        ).fetchone():
            raise LedgerError(f"来源不存在: {source}")
        if not conn.execute(
            "SELECT 1 FROM source_fields WHERE source_name = ? AND field_name = ?",
            (source, field),
        ).fetchone():
            raise LedgerError(f"字段不是该来源已声明的必需字段: {field}")
        if conn.execute(
            "SELECT 1 FROM field_rules"
            " WHERE source_name = ? AND field_name = ? AND rule_type = ?",
            (source, field, rule_type),
        ).fetchone():
            raise LedgerError(f"规则已存在: {source} {field} {rule_type}")
        conn.execute(
            "INSERT INTO field_rules"
            " (source_name, field_name, rule_type, min_value, max_value, candidates)"
            " VALUES (?, ?, ?, ?, ?, ?)",
            (source, field, rule_type, min_value, max_value, candidates),
        )
    return 1


def list_rules(conn: sqlite3.Connection, source: str) -> list[tuple[str, str, str]]:
    """按字段名、类型升序返回某来源的 (字段名, 类型, 边界或候选值)。

    decimal 无边界与 date 的第三列为 `-`；来源不存在时拒绝。
    """
    if not conn.execute(
        "SELECT 1 FROM sources WHERE name = ?", (source,)
    ).fetchone():
        raise LedgerError(f"来源不存在: {source}")
    rows = conn.execute(
        "SELECT field_name, rule_type, min_value, max_value, candidates"
        " FROM field_rules WHERE source_name = ?"
        " ORDER BY field_name, rule_type",
        (source,),
    ).fetchall()
    result = []
    for field_name, rule_type, min_value, max_value, candidates in rows:
        if rule_type == "enum":
            detail = candidates
        elif rule_type == "decimal" and (
            min_value is not None or max_value is not None
        ):
            detail = f"{min_value or ''}~{max_value or ''}"
        else:
            detail = "-"
        result.append((field_name, rule_type, detail))
    return result


def load_rules(
    conn: sqlite3.Connection, source: str
) -> list[tuple[str, str, str | None, str | None, tuple[str, ...] | None]]:
    """按注册顺序返回某来源的规则，候选值已拆分为元组，供导入时逐行校验。"""
    rows = conn.execute(
        "SELECT field_name, rule_type, min_value, max_value, candidates"
        " FROM field_rules WHERE source_name = ? ORDER BY rowid",
        (source,),
    ).fetchall()
    return [
        (
            field_name,
            rule_type,
            min_value,
            max_value,
            tuple(candidates.split(",")) if candidates is not None else None,
        )
        for field_name, rule_type, min_value, max_value, candidates in rows
    ]


def check_rule(
    value: str,
    rule_type: str,
    min_value: str | None,
    max_value: str | None,
    candidates: tuple[str, ...] | None,
) -> bool:
    """按单条规则校验字段值；任一条件不满足即返回 False。"""
    if rule_type == "decimal":
        if not is_decimal_value(value):
            return False
        number = Decimal(value)
        if min_value is not None and number < Decimal(min_value):
            return False
        if max_value is not None and number > Decimal(max_value):
            return False
        return True
    if rule_type == "date":
        return is_iso_date(value)
    return value in candidates  # enum：与候选值完全相等才通过


def list_batches(
    conn: sqlite3.Connection, source: str
) -> list[tuple[int, int, int, int, int, int]]:
    """按批次号升序返回某来源每批的计数。

    每项为 (批次号, 总行数, 成功导入数, 重复丢弃数, 校验失败数, 增量新数据数)。
    来源未注册时抛 LedgerError；无批次时返回空列表。
    """
    if not conn.execute(
        "SELECT 1 FROM sources WHERE name = ?", (source,)
    ).fetchone():
        raise LedgerError(f"来源不存在: {source}")
    return conn.execute(
        "SELECT batch_no, total_rows, imported_rows, duplicate_rows,"
        " rejected_rows, incremental_rows FROM import_batches"
        " WHERE source_name = ? ORDER BY batch_no",
        (source,),
    ).fetchall()


def rollback_batch(conn: sqlite3.Connection, source: str, batch_no: int) -> int:
    """撤销指定批次，返回该批次记录的总行数。

    删除批次记录及其当时新增的导入记录签名（仅该批签名，更早批次保留）。
    不产生拒绝文件。来源或批次不存在时抛 LedgerError 且无任何变更。
    """
    if not conn.execute(
        "SELECT 1 FROM sources WHERE name = ?", (source,)
    ).fetchone():
        raise LedgerError(f"来源不存在: {source}")
    row = conn.execute(
        "SELECT total_rows FROM import_batches"
        " WHERE source_name = ? AND batch_no = ?",
        (source, batch_no),
    ).fetchone()
    if row is None:
        raise LedgerError(f"批次不存在: {source} #{batch_no}")
    total_rows = row[0]
    with conn:  # 批次记录与该批签名单次原子提交，异常即回滚
        conn.execute(
            "DELETE FROM imported_records"
            " WHERE source_name = ? AND batch_no = ?",
            (source, batch_no),
        )
        conn.execute(
            "DELETE FROM import_batches WHERE source_name = ? AND batch_no = ?",
            (source, batch_no),
        )
    return total_rows
