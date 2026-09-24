"""SQLite 持久化：来源配置与字段映射台账。

仓库根目录下的 import_ledger.db 是唯一持久化载体，仅使用 sqlite3 标准库。
"""

from __future__ import annotations

import os
import sqlite3
from collections.abc import Sequence
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
    batch_num INTEGER NOT NULL,
    source_name TEXT NOT NULL REFERENCES sources (name),
    status TEXT NOT NULL,
    success_count INTEGER NOT NULL,
    quarantined_count INTEGER NOT NULL,
    PRIMARY KEY (source_name, batch_num)
);
CREATE TABLE IF NOT EXISTS imported_records (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_name TEXT NOT NULL,
    batch_num INTEGER NOT NULL,
    target_column TEXT NOT NULL,
    value TEXT NOT NULL
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


def get_source(conn: sqlite3.Connection, name: str) -> tuple[str, list[str]]:
    """返回 (CSV 路径, 必需字段名列表)；来源不存在时拒绝。"""
    row = conn.execute(
        "SELECT csv_path FROM sources WHERE name = ?", (name,)
    ).fetchone()
    if row is None:
        raise LedgerError(f"来源不存在: {name}")
    fields = [
        r[0]
        for r in conn.execute(
            "SELECT field_name FROM source_fields WHERE source_name = ?"
            " ORDER BY rowid",
            (name,),
        )
    ]
    return row[0], fields


def record_batch(
    conn: sqlite3.Connection,
    source: str,
    status: str,
    success_count: int,
    quarantined_count: int,
    records: Sequence[dict[str, str]] = (),
) -> int:
    """登记一个导入批次并（仅成功时）写入数据行，原子提交，返回批次号。

    批次号在同一来源下从 1 开始按创建顺序递增。被拒绝批次只登记台账，
    不写入任何数据行。
    """
    with conn:
        row = conn.execute(
            "SELECT COALESCE(MAX(batch_num), 0) FROM import_batches"
            " WHERE source_name = ?",
            (source,),
        ).fetchone()
        batch_num = row[0] + 1
        conn.execute(
            "INSERT INTO import_batches (batch_num, source_name, status,"
            " success_count, quarantined_count) VALUES (?, ?, ?, ?, ?)",
            (batch_num, source, status, success_count, quarantined_count),
        )
        if status == "ok" and records:
            conn.executemany(
                "INSERT INTO imported_records (source_name, batch_num,"
                " target_column, value) VALUES (?, ?, ?, ?)",
                [
                    (source, batch_num, target_column, value)
                    for record in records
                    for target_column, value in record.items()
                ],
            )
    return batch_num


def list_batches(
    conn: sqlite3.Connection, source: str
) -> list[tuple[int, str, int, int]]:
    """按批次号升序返回某来源的 (批次号, 状态, 成功行数, 被隔离行数)。"""
    if not conn.execute(
        "SELECT 1 FROM sources WHERE name = ?", (source,)
    ).fetchone():
        raise LedgerError(f"来源不存在: {source}")
    return conn.execute(
        "SELECT batch_num, status, success_count, quarantined_count"
        " FROM import_batches WHERE source_name = ? ORDER BY batch_num",
        (source,),
    ).fetchall()


def get_batch(
    conn: sqlite3.Connection, source: str, batch_num: int
) -> tuple[int, str, int, int]:
    """返回单个批次 (批次号, 状态, 成功行数, 被隔离行数)；不存在时拒绝。"""
    row = conn.execute(
        "SELECT batch_num, status, success_count, quarantined_count"
        " FROM import_batches WHERE source_name = ? AND batch_num = ?",
        (source, batch_num),
    ).fetchone()
    if row is None:
        raise LedgerError(f"批次不存在: {source} {batch_num}")
    return row
