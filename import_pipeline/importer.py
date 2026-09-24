"""批次导入：读取来源 CSV，按字段映射导入并持久化结果台账。

预检失败（来源未注册、CSV 不存在或无法读取、含未映射目标列、必需字段
未全部映射）抛 LedgerError，且不产生批次记录、拒绝文件或任何数据库变更。
"""

from __future__ import annotations

import csv
import hashlib
import sqlite3
from dataclasses import dataclass
from pathlib import Path

from .ledger import LedgerError


@dataclass
class ImportResult:
    """单批次导入的结果计数。"""

    source: str
    batch_no: int
    total_rows: int
    imported_rows: int
    duplicate_rows: int
    rejected_rows: int
    incremental_rows: int


def _row_signature(record: dict[str, str]) -> str:
    """按目标列名字典序升序拼接 `目标字段名=原值`，以 & 连接后取 sha256。"""
    payload = "&".join(f"{name}={record[name]}" for name in sorted(record))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def import_source(
    conn: sqlite3.Connection, source: str, reject_dir: Path | None = None
) -> ImportResult:
    """执行一个批次的导入，返回结果计数；预检失败抛 LedgerError。"""
    row = conn.execute(
        "SELECT csv_path FROM sources WHERE name = ?", (source,)
    ).fetchone()
    if row is None:
        raise LedgerError(f"来源不存在: {source}")
    csv_path = Path(row[0])
    if not csv_path.is_file():
        raise LedgerError(f"CSV 文件不存在: {csv_path}")
    try:
        with csv_path.open("r", encoding="utf-8", newline="") as fh:
            rows = list(csv.reader(fh))
    except (OSError, UnicodeError, csv.Error) as exc:
        raise LedgerError(f"CSV 文件无法读取: {csv_path}") from exc

    mappings = conn.execute(
        "SELECT source_column, target_column FROM field_mappings"
        " WHERE source_name = ? ORDER BY rowid",
        (source,),
    ).fetchall()
    mapping = dict(mappings)
    required = [
        r[0]
        for r in conn.execute(
            "SELECT field_name FROM source_fields WHERE source_name = ?"
            " ORDER BY rowid",
            (source,),
        )
    ]

    header: list[str] = rows[0] if rows else []
    data_rows = rows[1:] if rows else []
    for column in header:
        if column not in mapping:
            raise LedgerError(f"CSV 含未映射目标列: {column}")
    mapped_targets = set(mapping.values())
    for field in required:
        if field not in mapped_targets:
            raise LedgerError(f"必需字段未全部映射: {field}")

    seen = {
        r[0]
        for r in conn.execute(
            "SELECT signature FROM imported_records WHERE source_name = ?",
            (source,),
        )
    }

    imported = 0
    duplicates = 0
    rejected: list[list[str]] = []
    new_signatures: list[str] = []
    for raw in data_rows:
        if len(raw) != len(header):
            rejected.append(raw)
            continue
        record = {mapping[column]: value for column, value in zip(header, raw)}
        if any(not record.get(field) for field in required):
            rejected.append(raw)
            continue
        signature = _row_signature(record)
        if signature in seen:
            duplicates += 1
            continue
        seen.add(signature)
        new_signatures.append(signature)
        imported += 1

    previous = conn.execute(
        "SELECT MAX(batch_no) FROM import_batches WHERE source_name = ?",
        (source,),
    ).fetchone()[0]
    batch_no = (previous or 0) + 1

    with conn:  # 批次记录与导入记录单次原子提交，异常即回滚
        conn.execute(
            "INSERT INTO import_batches (source_name, batch_no, total_rows,"
            " imported_rows, duplicate_rows, rejected_rows, incremental_rows)"
            " VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                source,
                batch_no,
                len(data_rows),
                imported,
                duplicates,
                len(rejected),
                imported,
            ),
        )
        conn.executemany(
            "INSERT INTO imported_records (source_name, signature, batch_no)"
            " VALUES (?, ?, ?)",
            [(source, signature, batch_no) for signature in new_signatures],
        )

    if rejected:
        reject_path = (reject_dir or Path.cwd()) / f"rejected_{source}_{batch_no}.csv"
        with reject_path.open("w", encoding="utf-8", newline="") as fh:
            writer = csv.writer(fh)
            writer.writerow(header)
            writer.writerows(rejected)

    return ImportResult(
        source=source,
        batch_no=batch_no,
        total_rows=len(data_rows),
        imported_rows=imported,
        duplicate_rows=duplicates,
        rejected_rows=len(rejected),
        incremental_rows=imported,
    )
