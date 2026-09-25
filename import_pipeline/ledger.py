"""SQLite 持久化：来源配置与字段映射台账。

仓库根目录下的 import_ledger.db 是唯一持久化载体，仅使用 sqlite3 标准库。
"""

from __future__ import annotations

import csv
import json
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
CREATE TABLE IF NOT EXISTS batches (
    source_name TEXT NOT NULL REFERENCES sources (name),
    batch_no INTEGER NOT NULL,
    status TEXT NOT NULL,
    succeeded_rows INTEGER NOT NULL,
    quarantined_rows INTEGER NOT NULL,
    PRIMARY KEY (source_name, batch_no)
);
CREATE TABLE IF NOT EXISTS imported_rows (
    source_name TEXT NOT NULL,
    batch_no INTEGER NOT NULL,
    row_number INTEGER NOT NULL,
    data TEXT NOT NULL,
    PRIMARY KEY (source_name, batch_no, row_number),
    FOREIGN KEY (source_name, batch_no)
        REFERENCES batches (source_name, batch_no)
);
CREATE TABLE IF NOT EXISTS quarantined_rows (
    source_name TEXT NOT NULL,
    batch_no INTEGER NOT NULL,
    row_number INTEGER NOT NULL,
    reason TEXT NOT NULL,
    data TEXT NOT NULL,
    PRIMARY KEY (source_name, batch_no, row_number),
    FOREIGN KEY (source_name, batch_no)
        REFERENCES batches (source_name, batch_no)
);
"""

# 增量导入的业务键目标列与隔离原因字面值（只接受小写）。
BUSINESS_KEY_FIELD = "id"
REASON_BLANK_VALUE = "blank-value"
REASON_DUPLICATE_KEY = "duplicate-key"


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


def _record_batch(
    conn: sqlite3.Connection,
    source: str,
    status: str,
    succeeded_rows: int,
    quarantined_rows: int,
) -> int:
    """登记批次记录，返回分配的批次号（来源内从 1 递增）。调用方负责事务。"""
    batch_no = conn.execute(
        "SELECT COALESCE(MAX(batch_no), 0) + 1 FROM batches WHERE source_name = ?",
        (source,),
    ).fetchone()[0]
    conn.execute(
        "INSERT INTO batches"
        " (source_name, batch_no, status, succeeded_rows, quarantined_rows)"
        " VALUES (?, ?, ?, ?, ?)",
        (source, batch_no, status, succeeded_rows, quarantined_rows),
    )
    return batch_no


def _load_csv_source(
    conn: sqlite3.Connection, source: str
) -> tuple[dict[str, str], list[str], list[str] | None, list[list[str]]]:
    """读取来源配置与 CSV 内容，返回 (源列到目标列映射, 必需字段, 表头, 数据行)。

    来源不存在或 CSV 文件不存在/读取失败时抛 LedgerError（不留批次记录）。
    """
    row = conn.execute(
        "SELECT csv_path FROM sources WHERE name = ?", (source,)
    ).fetchone()
    if row is None:
        raise LedgerError(f"来源不存在: {source}")
    csv_path = row[0]
    if not Path(csv_path).is_file():
        raise LedgerError(f"CSV 文件不存在: {csv_path}")

    mappings = list_mappings(conn, source)
    target_by_source = dict(mappings)
    required_fields = [
        r[0]
        for r in conn.execute(
            "SELECT field_name FROM source_fields WHERE source_name = ?"
            " ORDER BY rowid",
            (source,),
        )
    ]

    try:
        with open(csv_path, newline="", encoding="utf-8") as handle:
            rows = list(csv.reader(handle))
    except OSError as exc:
        raise LedgerError(f"CSV 文件读取失败: {csv_path}: {exc}") from exc

    header: list[str] | None = rows[0] if rows else None
    data_rows = rows[1:] if rows else []
    return target_by_source, required_fields, header, data_rows


def _validate_header(
    target_by_source: dict[str, str],
    required_fields: Sequence[str],
    header: list[str] | None,
) -> str | None:
    """校验表头，返回错误信息；无问题时返回 None。"""
    if header is None:
        return "CSV 文件缺少表头"
    for column in header:
        if column not in target_by_source:
            return f"未映射的源列: {column}"
    produced = {target_by_source[column] for column in header}
    for field in required_fields:
        if field not in produced:
            return f"缺少映射目标对应列: {field}"
    return None


def _reject_batch(
    conn: sqlite3.Connection, source: str, data_rows: Sequence[list[str]], error: str
) -> None:
    """登记 rejected 批次后抛 LedgerError；已校验行不落库。"""
    with conn:  # 拒绝也留批次记录
        _record_batch(conn, source, "rejected", 0, len(data_rows))
    raise LedgerError(error)


def run_import(conn: sqlite3.Connection, source: str) -> int:
    """执行一次导入，返回成功行数。

    文件不存在时不留批次记录；校验失败登记 rejected 批次后抛 LedgerError；
    成功时数据行与 ok 批次在同一事务内落库。
    """
    target_by_source, required_fields, header, data_rows = _load_csv_source(
        conn, source
    )

    error = _validate_header(target_by_source, required_fields, header)
    if error is None:
        for data_row in data_rows:
            if len(data_row) != len(header) or any(
                not value.strip() for value in data_row
            ):
                error = "数据行含空白值"
                break

    if error is not None:
        _reject_batch(conn, source, data_rows, error)

    with conn:  # 数据行与批次记录原子提交
        batch_no = _record_batch(conn, source, "ok", len(data_rows), 0)
        conn.executemany(
            "INSERT INTO imported_rows"
            " (source_name, batch_no, row_number, data) VALUES (?, ?, ?, ?)",
            [
                (
                    source,
                    batch_no,
                    index,
                    json.dumps(
                        {
                            target_by_source[column]: value
                            for column, value in zip(header, data_row)
                        },
                        ensure_ascii=False,
                    ),
                )
                for index, data_row in enumerate(data_rows, start=1)
            ],
        )
    return len(data_rows)


def delta_import(conn: sqlite3.Connection, source: str) -> tuple[int, int, int]:
    """执行一次增量导入，返回 (新增行数, 跳过行数, 被隔离行数)。

    以来源配置声明的必需字段 id 作为业务键（字符串精确匹配，不做归一）：
    与此前任何批次已落库行同键的行跳过；本批内键冲突的行与值为空白的行
    逐行写入隔离记录。存在被隔离行时批次状态为 failed，否则为 ok。
    整批级问题登记 rejected 批次后抛 LedgerError。
    """
    target_by_source, required_fields, header, data_rows = _load_csv_source(
        conn, source
    )

    error = _validate_header(target_by_source, required_fields, header)
    if error is None and BUSINESS_KEY_FIELD not in {
        target_by_source[column] for column in header
    }:
        error = f"缺少映射目标对应列: {BUSINESS_KEY_FIELD}"
    if error is not None:
        _reject_batch(conn, source, data_rows, error)

    existing_keys = {
        key
        for (data,) in conn.execute(
            "SELECT data FROM imported_rows WHERE source_name = ?", (source,)
        )
        if (key := json.loads(data).get(BUSINESS_KEY_FIELD)) is not None
    }

    quarantine: list[tuple[int, str, list[str]]] = []  # (行号, 原因, 原始行)
    candidates: list[tuple[int, str, list[str]]] = []  # (行号, 业务键, 原始行)
    for index, data_row in enumerate(data_rows, start=1):
        if len(data_row) != len(header) or any(
            not value.strip() for value in data_row
        ):
            quarantine.append((index, REASON_BLANK_VALUE, data_row))
        else:
            record = {
                target_by_source[column]: value
                for column, value in zip(header, data_row)
            }
            candidates.append((index, record[BUSINESS_KEY_FIELD], data_row))

    key_counts: dict[str, int] = {}
    for _, key, _ in candidates:
        key_counts[key] = key_counts.get(key, 0) + 1
    conflict_keys = {key for key, count in key_counts.items() if count > 1}

    skipped = 0
    inserts: list[tuple[int, list[str]]] = []
    for index, key, data_row in candidates:
        if key in conflict_keys:
            quarantine.append((index, REASON_DUPLICATE_KEY, data_row))
        elif key in existing_keys:
            skipped += 1
        else:
            inserts.append((index, data_row))
            existing_keys.add(key)

    status = "failed" if quarantine else "ok"
    with conn:  # 数据行、隔离记录与批次记录原子提交
        batch_no = _record_batch(conn, source, status, len(inserts), len(quarantine))
        conn.executemany(
            "INSERT INTO imported_rows"
            " (source_name, batch_no, row_number, data) VALUES (?, ?, ?, ?)",
            [
                (
                    source,
                    batch_no,
                    index,
                    json.dumps(
                        {
                            target_by_source[column]: value
                            for column, value in zip(header, data_row)
                        },
                        ensure_ascii=False,
                    ),
                )
                for index, data_row in inserts
            ],
        )
        conn.executemany(
            "INSERT INTO quarantined_rows"
            " (source_name, batch_no, row_number, reason, data)"
            " VALUES (?, ?, ?, ?, ?)",
            [
                (
                    source,
                    batch_no,
                    index,
                    reason,
                    json.dumps(
                        {
                            target_by_source[column]: value
                            for column, value in zip(header, data_row)
                        },
                        ensure_ascii=False,
                    ),
                )
                for index, reason, data_row in quarantine
            ],
        )
    return len(inserts), skipped, len(quarantine)


def list_quarantine(
    conn: sqlite3.Connection, source: str
) -> list[tuple[int, int, str, str]]:
    """返回某来源全部隔离记录：(批次号, 行号, 原因, 列名=值 对)。

    列名=值 对按目标列名排序、逗号连接；记录按批次号与行号升序。
    来源不存在时拒绝。
    """
    if not conn.execute(
        "SELECT 1 FROM sources WHERE name = ?", (source,)
    ).fetchone():
        raise LedgerError(f"来源不存在: {source}")
    rows = conn.execute(
        "SELECT batch_no, row_number, reason, data FROM quarantined_rows"
        " WHERE source_name = ? ORDER BY batch_no, row_number",
        (source,),
    ).fetchall()
    return [
        (
            batch_no,
            row_number,
            reason,
            ",".join(f"{key}={record[key]}" for key in sorted(record)),
        )
        for batch_no, row_number, reason, data in rows
        for record in [json.loads(data)]
    ]


def show_batches(
    conn: sqlite3.Connection, source: str, batch_no: int
) -> list[tuple[int, str, int, int]]:
    """返回某来源自 batch_no 起（含）的批次，按批次号升序。

    来源或批次号不存在时拒绝。
    """
    if not conn.execute(
        "SELECT 1 FROM sources WHERE name = ?", (source,)
    ).fetchone():
        raise LedgerError(f"来源不存在: {source}")
    if not conn.execute(
        "SELECT 1 FROM batches WHERE source_name = ? AND batch_no = ?",
        (source, batch_no),
    ).fetchone():
        raise LedgerError(f"批次不存在: {batch_no}")
    return conn.execute(
        "SELECT batch_no, status, succeeded_rows, quarantined_rows"
        " FROM batches WHERE source_name = ? AND batch_no >= ?"
        " ORDER BY batch_no",
        (source, batch_no),
    ).fetchall()
