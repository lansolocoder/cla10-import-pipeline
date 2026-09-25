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
