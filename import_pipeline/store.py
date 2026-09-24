"""SQLite-backed persistence for source and field-mapping configuration.

The database file (``import_ledger.db`` at the repository root) is the only
persistence artifact. Every write runs inside a single transaction so that a
rejected operation leaves previously saved configuration untouched.
"""

from __future__ import annotations

import json
import sqlite3
from contextlib import closing
from pathlib import Path
from typing import NamedTuple

DB_PATH = Path(__file__).resolve().parents[1] / "import_ledger.db"

SCHEMA = """
CREATE TABLE IF NOT EXISTS sources (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    path TEXT NOT NULL,
    fields_json TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS mappings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_id INTEGER NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
    source_column TEXT NOT NULL,
    target_column TEXT NOT NULL,
    UNIQUE(source_id, source_column)
);
"""


class ConfigError(Exception):
    """A configuration operation was rejected for a semantic reason."""


class ConflictError(ConfigError):
    """A unique business key is already registered."""


class NotFoundError(ConfigError):
    """The referenced source does not exist."""


class Source(NamedTuple):
    name: str
    path: str
    fields: list[str]


class Mapping(NamedTuple):
    source_column: str
    target_column: str


def _validate_source(name: str, path: str, fields: list[str]) -> None:
    if name == "":
        raise ConfigError("来源名不能为空")
    if path == "":
        raise ConfigError("CSV 文件路径不能为空")
    if not fields:
        raise ConfigError("必需字段至少需要一个")
    seen: set[str] = set()
    for field in fields:
        if field == "":
            raise ConfigError("字段名不能为空")
        if field in seen:
            raise ConfigError(f"字段名重复: {field}")
        seen.add(field)


def _write_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON")
    conn.executescript(SCHEMA)
    return conn


def add_source(*, name: str, path: str, fields: list[str]) -> None:
    """Register one source configuration atomically."""
    _validate_source(name, path, fields)
    with closing(_write_connection()) as conn:
        try:
            with conn:
                exists = conn.execute(
                    "SELECT 1 FROM sources WHERE name = ?", (name,)
                ).fetchone()
                if exists is not None:
                    raise ConflictError(f"来源名已存在: {name}")
                conn.execute(
                    "INSERT INTO sources (name, path, fields_json) VALUES (?, ?, ?)",
                    (name, path, json.dumps(fields, ensure_ascii=False)),
                )
        except sqlite3.IntegrityError:
            raise ConflictError(f"来源名已存在: {name}") from None


def add_mapping(
    *, source_name: str, source_column: str, target_column: str
) -> None:
    """Register one field mapping atomically; the first mapping wins."""
    if source_name == "":
        raise ConfigError("来源名不能为空")
    if source_column == "":
        raise ConfigError("源列名不能为空")
    if target_column == "":
        raise ConfigError("目标列名不能为空")
    with closing(_write_connection()) as conn:
        try:
            with conn:
                row = conn.execute(
                    "SELECT id FROM sources WHERE name = ?", (source_name,)
                ).fetchone()
                if row is None:
                    raise NotFoundError(f"来源不存在: {source_name}")
                source_id = row[0]
                duplicate = conn.execute(
                    "SELECT 1 FROM mappings "
                    "WHERE source_id = ? AND source_column = ?",
                    (source_id, source_column),
                ).fetchone()
                if duplicate is not None:
                    raise ConflictError(
                        f"来源 {source_name} 下源列名已存在映射: {source_column}"
                    )
                conn.execute(
                    "INSERT INTO mappings "
                    "(source_id, source_column, target_column) VALUES (?, ?, ?)",
                    (source_id, source_column, target_column),
                )
        except sqlite3.IntegrityError:
            raise ConflictError(
                f"来源 {source_name} 下源列名已存在映射: {source_column}"
            ) from None


def _read_connection() -> sqlite3.Connection | None:
    # Read-only commands must not create the database on a fresh checkout.
    if not DB_PATH.exists():
        return None
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def list_sources() -> list[Source]:
    conn = _read_connection()
    if conn is None:
        return []
    with closing(conn):
        rows = conn.execute(
            "SELECT name, path, fields_json FROM sources ORDER BY id"
        ).fetchall()
    return [
        Source(name, path, json.loads(fields_json)) for name, path, fields_json in rows
    ]


def list_mappings(source_name: str) -> list[Mapping]:
    conn = _read_connection()
    if conn is None:
        raise NotFoundError(f"来源不存在: {source_name}")
    with closing(conn):
        row = conn.execute(
            "SELECT id FROM sources WHERE name = ?", (source_name,)
        ).fetchone()
        if row is None:
            raise NotFoundError(f"来源不存在: {source_name}")
        rows = conn.execute(
            "SELECT source_column, target_column FROM mappings "
            "WHERE source_id = ? ORDER BY id",
            (row[0],),
        ).fetchall()
    return [Mapping(source_column, target_column) for source_column, target_column in rows]
