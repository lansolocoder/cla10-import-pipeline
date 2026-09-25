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


def _load_source(
    conn: sqlite3.Connection, source: str
) -> tuple[list[str], list[list[str]], dict[str, str], list[str]]:
    """预检并返回 (表头, 数据行, 映射, 必需字段)；失败抛 LedgerError。"""
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
    return header, data_rows, mapping, required


def _known_signatures(conn: sqlite3.Connection, source: str) -> set[str]:
    """该来源此前任意批次已导入成功的记录签名集合。"""
    return {
        r[0]
        for r in conn.execute(
            "SELECT signature FROM imported_records WHERE source_name = ?",
            (source,),
        )
    }


def _classify_rows(
    header: list[str],
    data_rows: list[list[str]],
    mapping: dict[str, str],
    required: list[str],
    seen: set[str],
) -> tuple[int, int, list[list[str]], list[str]]:
    """逐行分类，返回 (成功数, 重复数, 失败行, 新增签名)；seen 被就地更新。"""
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
    return imported, duplicates, rejected, new_signatures


def _write_rejects(
    reject_dir: Path | None,
    source: str,
    batch_no: int,
    header: list[str],
    rejected: list[list[str]],
) -> None:
    """有校验失败行时隔离到 rejected_<来源>_<批次号>.csv。"""
    if not rejected:
        return
    reject_path = (reject_dir or Path.cwd()) / f"rejected_{source}_{batch_no}.csv"
    with reject_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(header)
        writer.writerows(rejected)


def import_source(
    conn: sqlite3.Connection, source: str, reject_dir: Path | None = None
) -> ImportResult:
    """执行一个批次的导入，返回结果计数；预检失败抛 LedgerError。"""
    header, data_rows, mapping, required = _load_source(conn, source)
    seen = _known_signatures(conn, source)
    imported, duplicates, rejected, new_signatures = _classify_rows(
        header, data_rows, mapping, required, seen
    )

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

    _write_rejects(reject_dir, source, batch_no, header, rejected)

    return ImportResult(
        source=source,
        batch_no=batch_no,
        total_rows=len(data_rows),
        imported_rows=imported,
        duplicate_rows=duplicates,
        rejected_rows=len(rejected),
        incremental_rows=imported,
    )


def reimport_source(
    conn: sqlite3.Connection,
    source: str,
    batch_no: int,
    reject_dir: Path | None = None,
) -> ImportResult:
    """重跑指定批次：沿用导入行语义，原子地整体替换原批次记录，批次号不变。

    判重签名集合含被重跑批次自身的既有记录；批次不存在时抛 LedgerError，
    且不改动任何记录与文件。
    """
    if not conn.execute(
        "SELECT 1 FROM import_batches WHERE source_name = ? AND batch_no = ?",
        (source, batch_no),
    ).fetchone():
        raise LedgerError(f"批次不存在: {source} 批次号 {batch_no}")

    header, data_rows, mapping, required = _load_source(conn, source)
    seen = _known_signatures(conn, source)
    imported, duplicates, rejected, new_signatures = _classify_rows(
        header, data_rows, mapping, required, seen
    )

    with conn:  # 删除旧记录、更新批次计数、写入新签名，单次原子提交
        conn.execute(
            "DELETE FROM imported_records WHERE source_name = ? AND batch_no = ?",
            (source, batch_no),
        )
        conn.execute(
            "UPDATE import_batches SET total_rows = ?, imported_rows = ?,"
            " duplicate_rows = ?, rejected_rows = ?, incremental_rows = ?"
            " WHERE source_name = ? AND batch_no = ?",
            (
                len(data_rows),
                imported,
                duplicates,
                len(rejected),
                imported,
                source,
                batch_no,
            ),
        )
        conn.executemany(
            "INSERT INTO imported_records (source_name, signature, batch_no)"
            " VALUES (?, ?, ?)",
            [(source, signature, batch_no) for signature in new_signatures],
        )

    _write_rejects(reject_dir, source, batch_no, header, rejected)

    return ImportResult(
        source=source,
        batch_no=batch_no,
        total_rows=len(data_rows),
        imported_rows=imported,
        duplicate_rows=duplicates,
        rejected_rows=len(rejected),
        incremental_rows=imported,
    )


def rollback_batch(conn: sqlite3.Connection, source: str, batch_no: int) -> int:
    """撤销指定批次：删除批次记录及其当时新增的签名，返回该批总行数。

    更早批次与其他批次不受影响；批次不存在时抛 LedgerError 且无任何变更。
    """
    row = conn.execute(
        "SELECT total_rows FROM import_batches"
        " WHERE source_name = ? AND batch_no = ?",
        (source, batch_no),
    ).fetchone()
    if row is None:
        raise LedgerError(f"批次不存在: {source} 批次号 {batch_no}")
    with conn:  # 批次记录与其新增签名单次原子删除
        conn.execute(
            "DELETE FROM imported_records WHERE source_name = ? AND batch_no = ?",
            (source, batch_no),
        )
        conn.execute(
            "DELETE FROM import_batches WHERE source_name = ? AND batch_no = ?",
            (source, batch_no),
        )
    return row[0]
