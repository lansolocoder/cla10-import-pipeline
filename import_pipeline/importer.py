"""批次导入：读取来源 CSV，按字段映射导入并持久化结果台账。

预检失败（来源未注册、CSV 不存在或无法读取、含未映射目标列、必需字段
未全部映射）抛 LedgerError，且不产生批次记录、拒绝文件或任何数据库变更。
"""

from __future__ import annotations

import csv
import hashlib
import re
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


@dataclass
class FixRejectsResult:
    """fix-rejects 的结果：可疑行数。"""

    source: str
    batch_no: int
    suspicious_rows: int


@dataclass
class ApplyFixedResult:
    """apply-fixed 的结果计数：修复成功数、重复丢弃数、拒绝数。"""

    source: str
    batch_no: int
    imported_rows: int
    duplicate_rows: int
    rejected_rows: int


def _row_signature(record: dict[str, str]) -> str:
    """按目标列名字典序升序拼接 `目标字段名=原值`，以 & 连接后取 sha256。"""
    payload = "&".join(f"{name}={record[name]}" for name in sorted(record))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _load_mapping(
    conn: sqlite3.Connection, source: str
) -> tuple[dict[str, str], list[str]]:
    """读取来源的 源列->目标列映射 与必需字段列表（不校验来源存在性）。"""
    mappings = conn.execute(
        "SELECT source_column, target_column FROM field_mappings"
        " WHERE source_name = ? ORDER BY rowid",
        (source,),
    ).fetchall()
    required = [
        r[0]
        for r in conn.execute(
            "SELECT field_name FROM source_fields WHERE source_name = ?"
            " ORDER BY rowid",
            (source,),
        )
    ]
    return dict(mappings), required


def _check_mapping(
    header: list[str], mapping: dict[str, str], required: list[str]
) -> None:
    """表头列须全部已映射且必需字段全部已映射，否则抛 LedgerError。"""
    for column in header:
        if column not in mapping:
            raise LedgerError(f"CSV 含未映射目标列: {column}")
    mapped_targets = set(mapping.values())
    for field in required:
        if field not in mapped_targets:
            raise LedgerError(f"必需字段未全部映射: {field}")


def _prepare(
    conn: sqlite3.Connection, source: str
) -> tuple[list[list[str]], list[str], dict[str, str], list[str]]:
    """预检并返回 (数据行, 表头, 源列->目标列映射, 必需字段列表)。"""
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

    mapping, required = _load_mapping(conn, source)
    header: list[str] = rows[0] if rows else []
    data_rows = rows[1:] if rows else []
    _check_mapping(header, mapping, required)

    return data_rows, header, mapping, required


def _build_record(
    raw: list[str], header: list[str], mapping: dict[str, str]
) -> dict[str, str]:
    return {mapping[column]: value for column, value in zip(header, raw)}


def _passes_validation(
    raw: list[str], header: list[str], mapping: dict[str, str], required: list[str]
) -> bool:
    """行校验：列数与表头一致且必需字段值均非空（判定先于判重）。"""
    if len(raw) != len(header):
        return False
    record = _build_record(raw, header, mapping)
    return all(record.get(field) for field in required)


def _classify_rows(
    data_rows: list[list[str]],
    header: list[str],
    mapping: dict[str, str],
    required: list[str],
    seen: set[str],
) -> tuple[int, int, list[list[str]], list[str]]:
    """按既有行语义分类，返回 (成功数, 重复数, 失败行, 增量新签名)。"""
    imported = 0
    duplicates = 0
    rejected: list[list[str]] = []
    new_signatures: list[str] = []
    for raw in data_rows:
        if len(raw) != len(header):
            rejected.append(raw)
            continue
        record = _build_record(raw, header, mapping)
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


def _write_rejected(
    rejected: list[list[str]], header: list[str], path: Path
) -> None:
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(header)
        writer.writerows(rejected)


def import_source(
    conn: sqlite3.Connection, source: str, reject_dir: Path | None = None
) -> ImportResult:
    """执行一个批次的导入，返回结果计数；预检失败抛 LedgerError。"""
    data_rows, header, mapping, required = _prepare(conn, source)

    seen = {
        r[0]
        for r in conn.execute(
            "SELECT signature FROM imported_records WHERE source_name = ?",
            (source,),
        )
    }
    imported, duplicates, rejected, new_signatures = _classify_rows(
        data_rows, header, mapping, required, seen
    )

    previous = conn.execute(
        "SELECT MAX(batch_no) FROM import_batches WHERE source_name = ?",
        (source,),
    ).fetchone()[0]
    batch_no = (previous or 0) + 1

    with conn:  # 批次记录与导入记录单次原子提交，异常即回滚
        conn.execute(
            "INSERT INTO import_batches (source_name, batch_no, total_rows,"
            " imported_rows, duplicate_rows, rejected_rows, incremental_rows,"
            " origin)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, 'import')",
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
        _write_rejected(rejected, header, reject_path)

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
    """显式重跑指定批次：沿用全部导进行语义，原子替换原批次记录，批次号不变。

    签名判重与同来源当前全部已导入成功记录比较（含被重跑批次自身）。
    来源或目标批次不存在、预检失败时抛 LedgerError，且不改动任何记录与文件。
    """
    if conn.execute(
        "SELECT 1 FROM sources WHERE name = ?", (source,)
    ).fetchone() is None:
        raise LedgerError(f"来源不存在: {source}")
    if conn.execute(
        "SELECT 1 FROM import_batches WHERE source_name = ? AND batch_no = ?",
        (source, batch_no),
    ).fetchone() is None:
        raise LedgerError(f"批次不存在: {source} #{batch_no}")

    data_rows, header, mapping, required = _prepare(conn, source)

    seen = {
        r[0]
        for r in conn.execute(
            "SELECT signature FROM imported_records WHERE source_name = ?",
            (source,),
        )
    }
    imported, duplicates, rejected, new_signatures = _classify_rows(
        data_rows, header, mapping, required, seen
    )

    with conn:  # 删除旧签名、替换批次计数、写入新签名单次原子提交
        conn.execute(
            "DELETE FROM imported_records"
            " WHERE source_name = ? AND batch_no = ?",
            (source, batch_no),
        )
        conn.execute(
            "UPDATE import_batches SET total_rows = ?, imported_rows = ?,"
            " duplicate_rows = ?, rejected_rows = ?, incremental_rows = ?,"
            " origin = 'reimport'"
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

    if rejected:
        reject_path = (reject_dir or Path.cwd()) / f"rejected_{source}_{batch_no}.csv"
        _write_rejected(rejected, header, reject_path)

    return ImportResult(
        source=source,
        batch_no=batch_no,
        total_rows=len(data_rows),
        imported_rows=imported,
        duplicate_rows=duplicates,
        rejected_rows=len(rejected),
        incremental_rows=imported,
    )


_FIX_RESULT_COLUMN = "fix_result"
_FIX_RESULT_PATTERN = re.compile(r"fix_result(?:_[1-9][0-9]*)?")


def _reject_path(source: str, batch_no: int, directory: Path) -> Path:
    return directory / f"rejected_{source}_{batch_no}.csv"


def _fix_path(source: str, batch_no: int, directory: Path) -> Path:
    return directory / f"rejected_{source}_{batch_no}_fix.csv"


def _fix_column_name(header: list[str]) -> str:
    """追加的复核结果列名：原表头含 fix_result 时取首个不冲突的 fix_result_N。"""
    if _FIX_RESULT_COLUMN not in header:
        return _FIX_RESULT_COLUMN
    suffix = 1
    while f"{_FIX_RESULT_COLUMN}_{suffix}" in header:
        suffix += 1
    return f"{_FIX_RESULT_COLUMN}_{suffix}"


def fix_rejects(
    conn: sqlite3.Connection,
    source: str,
    batch_no: int,
    reject_dir: Path | None = None,
) -> FixRejectsResult:
    """导出指定批次的可疑行供人工复核，返回可疑行数。

    批次须存在且由 import 创建（reimport 批次拒绝）；可疑行数为零时同样拒绝，
    均不产生任何文件与数据库变更。否则重写当前目录的拒绝文件，并生成同目录
    `<拒绝文件名去 .csv>_fix.csv`：表头为原表头追加空列 fix_result（冲突时取
    首个不冲突的 fix_result_N），可疑行原样按出现顺序在前、fix_result 留空。
    不改动批次计数与签名。
    """
    if conn.execute(
        "SELECT 1 FROM sources WHERE name = ?", (source,)
    ).fetchone() is None:
        raise LedgerError(f"来源不存在: {source}")
    row = conn.execute(
        "SELECT origin FROM import_batches"
        " WHERE source_name = ? AND batch_no = ?",
        (source, batch_no),
    ).fetchone()
    if row is None:
        raise LedgerError(f"批次不存在: {source} #{batch_no}")
    if row[0] != "import":
        raise LedgerError(f"批次由 reimport 创建，无法行复核: {source} #{batch_no}")

    data_rows, header, mapping, required = _prepare(conn, source)
    suspicious = [
        raw
        for raw in data_rows
        if not _passes_validation(raw, header, mapping, required)
    ]
    if not suspicious:
        raise LedgerError(f"可疑行数为 0，无需行复核: {source} #{batch_no}")

    directory = reject_dir or Path.cwd()
    _write_rejected(suspicious, header, _reject_path(source, batch_no, directory))
    fix_column = _fix_column_name(header)
    with _fix_path(source, batch_no, directory).open(
        "w", encoding="utf-8", newline=""
    ) as fh:
        writer = csv.writer(fh)
        writer.writerow([*header, fix_column])
        for raw in suspicious:
            writer.writerow([*raw, ""])

    return FixRejectsResult(
        source=source, batch_no=batch_no, suspicious_rows=len(suspicious)
    )


def apply_fixed(
    conn: sqlite3.Connection,
    source: str,
    batch_no: int,
    reject_dir: Path | None = None,
) -> ApplyFixedResult:
    """应用人工复核结果：导入修复成功行、隔离拒绝行并单事务更新批次计数。

    修复文件（rejected_<来源>_<批次号>_fix.csv）的 fix_result（或 fix_result_N）
    列只接受小写 ok 或 rejected；其他取值或缺列、ok 行重新行校验仍不过关时
    抛 LedgerError 且无任何变更。ok 行按同来源全部历史签名判重：命中计重复
    丢弃，否则导入为增量新数据；rejected 行按修复文件出现顺序写回同名 fix
    结果文件（表头与原 CSV 一致，无 rejected 行则不生成）。批次计数单事务
    更新（总行数不变），批次号不变；成功后删除修复文件，原拒绝文件始终保留。
    批次不存在、未经 fix-rejects、修复文件缺失时抛 LedgerError 且无任何变更。
    """
    if conn.execute(
        "SELECT 1 FROM sources WHERE name = ?", (source,)
    ).fetchone() is None:
        raise LedgerError(f"来源不存在: {source}")
    if conn.execute(
        "SELECT 1 FROM import_batches WHERE source_name = ? AND batch_no = ?",
        (source, batch_no),
    ).fetchone() is None:
        raise LedgerError(f"批次不存在: {source} #{batch_no}")

    directory = reject_dir or Path.cwd()
    fix_path = _fix_path(source, batch_no, directory)
    if not fix_path.is_file():
        raise LedgerError(f"修复文件缺失（需先执行 fix-rejects）: {fix_path}")
    try:
        with fix_path.open("r", encoding="utf-8", newline="") as fh:
            rows = list(csv.reader(fh))
    except (OSError, UnicodeError, csv.Error) as exc:
        raise LedgerError(f"修复文件无法读取: {fix_path}") from exc

    fix_header: list[str] = rows[0] if rows else []
    if not fix_header or not _FIX_RESULT_PATTERN.fullmatch(fix_header[-1]):
        raise LedgerError(f"修复文件缺少 fix_result 列: {fix_path}")
    header = fix_header[:-1]
    fix_index = len(fix_header) - 1

    mapping, required = _load_mapping(conn, source)
    _check_mapping(header, mapping, required)

    entries: list[tuple[str, list[str]]] = []
    for raw in rows[1:]:
        decision = raw[fix_index] if fix_index < len(raw) else ""
        if decision not in ("ok", "rejected"):
            raise LedgerError(
                f"fix_result 列取值非法（只接受 ok 或 rejected）: {decision!r}"
            )
        entries.append((decision, raw[:fix_index] + raw[fix_index + 1 :]))

    for decision, data in entries:
        if decision == "ok" and not _passes_validation(
            data, header, mapping, required
        ):
            raise LedgerError(f"修复行仍未通过校验: {','.join(data)}")

    seen = {
        r[0]
        for r in conn.execute(
            "SELECT signature FROM imported_records WHERE source_name = ?",
            (source,),
        )
    }
    imported = 0
    duplicates = 0
    new_signatures: list[str] = []
    rejected_rows: list[list[str]] = []
    for decision, data in entries:
        if decision == "rejected":
            rejected_rows.append(data)
            continue
        signature = _row_signature(_build_record(data, header, mapping))
        if signature in seen:
            duplicates += 1
            continue
        seen.add(signature)
        new_signatures.append(signature)
        imported += 1

    with conn:  # 批次计数更新与新签名单次原子提交，异常即回滚
        conn.execute(
            "UPDATE import_batches SET imported_rows = imported_rows + ?,"
            " duplicate_rows = duplicate_rows + ?,"
            " rejected_rows = rejected_rows - ?,"
            " incremental_rows = incremental_rows + ?"
            " WHERE source_name = ? AND batch_no = ?",
            (imported, duplicates, len(entries), imported, source, batch_no),
        )
        conn.executemany(
            "INSERT INTO imported_records (source_name, signature, batch_no)"
            " VALUES (?, ?, ?)",
            [(source, signature, batch_no) for signature in new_signatures],
        )

    fix_path.unlink()
    if rejected_rows:
        _write_rejected(rejected_rows, header, fix_path)

    return ApplyFixedResult(
        source=source,
        batch_no=batch_no,
        imported_rows=imported,
        duplicate_rows=duplicates,
        rejected_rows=len(rejected_rows),
    )
