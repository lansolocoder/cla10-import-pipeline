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

    return data_rows, header, mapping, required


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
            " created_by)"
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
            " created_by = 'reimport'"
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
        conn.execute(
            "DELETE FROM fix_reviews WHERE source_name = ? AND batch_no = ?",
            (source, batch_no),
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


@dataclass
class ApplyFixedResult:
    """apply-fixed 的结果计数。"""

    source: str
    batch_no: int
    fixed_rows: int
    duplicate_rows: int
    rejected_rows: int


def _reject_paths(
    reject_dir: Path | None, source: str, batch_no: int
) -> tuple[Path, Path]:
    """返回 (原拒绝文件, 复核文件)；复核文件路径在应用修复后兼作 fix 结果文件。"""
    directory = reject_dir or Path.cwd()
    return (
        directory / f"rejected_{source}_{batch_no}.csv",
        directory / f"rejected_{source}_{batch_no}_fix.csv",
    )


def _is_fix_column(name: str) -> bool:
    """fix_result 或 fix_result_N（N 为十进制数字）。"""
    if name == "fix_result":
        return True
    prefix = "fix_result_"
    return name.startswith(prefix) and name[len(prefix) :].isdigit()


def _nonconflicting_fix_column(header: list[str]) -> str:
    """返回不与既有表头冲突的修复结果列名（fix_result，冲突时 fix_result_N，N 从 1 起）。"""
    existing = set(header)
    if "fix_result" not in existing:
        return "fix_result"
    index = 1
    while f"fix_result_{index}" in existing:
        index += 1
    return f"fix_result_{index}"


def _find_fix_column(header: list[str]) -> int | None:
    """定位修复结果列：去掉该列后，它恰为“首个不冲突的 fix_result(_N)”。

    fix-rejects 总把该列追加在末尾；这里同时兼容人工调整过列序的修复文件。
    """
    candidates = [
        index
        for index, name in enumerate(header)
        if _is_fix_column(name)
    ]
    # 优先末尾列（工具生成的形态），再按列序回退。
    candidates.sort(key=lambda index: (index != len(header) - 1, index))
    for index in candidates:
        base = [column for j, column in enumerate(header) if j != index]
        if header[index] == _nonconflicting_fix_column(base):
            return index
    return None


def _read_csv(path: Path, label: str) -> list[list[str]]:
    try:
        with path.open("r", encoding="utf-8", newline="") as fh:
            return list(csv.reader(fh))
    except (OSError, UnicodeError, csv.Error) as exc:
        raise LedgerError(f"{label}无法读取: {path}") from exc


def _load_mapping(
    conn: sqlite3.Connection, source: str
) -> tuple[dict[str, str], list[str]]:
    """读取来源当前的源列->目标列映射与必需字段。"""
    mapping = dict(
        conn.execute(
            "SELECT source_column, target_column FROM field_mappings"
            " WHERE source_name = ? ORDER BY rowid",
            (source,),
        ).fetchall()
    )
    required = [
        row[0]
        for row in conn.execute(
            "SELECT field_name FROM source_fields WHERE source_name = ?"
            " ORDER BY rowid",
            (source,),
        )
    ]
    return mapping, required


def fix_rejects(
    conn: sqlite3.Connection,
    source: str,
    batch_no: int,
    reject_dir: Path | None = None,
) -> int:
    """导出某 import 批次的可疑行供人工复核，返回导出的可疑行数。

    重写 rejected_<来源>_<批次号>.csv（表头加可疑行、原样按出现顺序），
    并在同目录生成 rejected_<来源>_<批次号>_fix.csv（原表头追加修复结果列，
    可疑行原样在前、fix_result 留空；原行含 fix_result 列时用首个不冲突的
    fix_result_N）。批次不存在、由 reimport 创建或可疑行数为 0 时抛
    LedgerError（退出码 1），不生成文件、不改批次计数与签名。
    """
    if conn.execute(
        "SELECT 1 FROM sources WHERE name = ?", (source,)
    ).fetchone() is None:
        raise LedgerError(f"来源不存在: {source}")
    batch = conn.execute(
        "SELECT rejected_rows, created_by FROM import_batches"
        " WHERE source_name = ? AND batch_no = ?",
        (source, batch_no),
    ).fetchone()
    if batch is None:
        raise LedgerError(f"批次不存在: {source} #{batch_no}")
    rejected_count, created_by = batch
    if created_by == "reimport":
        raise LedgerError(
            f"批次由 reimport 创建，不支持行复核: {source} #{batch_no}"
        )
    if rejected_count == 0:
        raise LedgerError(f"批次无可疑行: {source} #{batch_no}")

    rejected_path, fix_path = _reject_paths(reject_dir, source, batch_no)
    if not rejected_path.is_file():
        raise LedgerError(f"拒绝文件不存在: {rejected_path}")
    rows = _read_csv(rejected_path, "拒绝文件")
    header = rows[0] if rows else []
    suspicious = rows[1:] if rows else []
    if not suspicious:
        raise LedgerError(f"拒绝文件无可疑行: {rejected_path}")

    fix_column = _nonconflicting_fix_column(header)
    # 先生成复核文件、再重写拒绝文件；均不影响数据库计数与签名。
    with fix_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow([*header, fix_column])
        for raw in suspicious:
            writer.writerow([*raw, ""])
    _write_rejected(suspicious, header, rejected_path)

    with conn:  # 仅记录“已生成复核文件”，不改批次计数与签名
        conn.execute(
            "INSERT OR IGNORE INTO fix_reviews (source_name, batch_no)"
            " VALUES (?, ?)",
            (source, batch_no),
        )
    return len(suspicious)


def apply_fixed(
    conn: sqlite3.Connection,
    source: str,
    batch_no: int,
    reject_dir: Path | None = None,
) -> ApplyFixedResult:
    """读回人工修复文件并应用：ok 行重新行校验、签名判重后增量导入。

    fix_result（或 fix_result_N）列只接受小写 ok/rejected；其他取值、缺列、
    ok 行重新校验不过关、批次不存在、未经 fix-rejects 或修复文件缺失时抛
    LedgerError（退出码 1）且无任何变更。成功时单事务更新批次计数（总行数
    不变）并写新签名，批次号不变；删除复核文件，原拒绝文件始终保留；
    rejected 行写入同路径的 fix 结果文件（表头与原 CSV 一致），无 rejected
    行则不生成结果文件。
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
    created_by = conn.execute(
        "SELECT created_by FROM import_batches"
        " WHERE source_name = ? AND batch_no = ?",
        (source, batch_no),
    ).fetchone()[0]
    if created_by == "reimport":
        raise LedgerError(
            f"批次由 reimport 创建，不支持行复核: {source} #{batch_no}"
        )
    if conn.execute(
        "SELECT 1 FROM fix_reviews WHERE source_name = ? AND batch_no = ?",
        (source, batch_no),
    ).fetchone() is None:
        raise LedgerError(
            f"批次未经 fix-rejects，不能应用修复: {source} #{batch_no}"
        )

    rejected_path, fix_path = _reject_paths(reject_dir, source, batch_no)
    if not fix_path.is_file():
        raise LedgerError(f"修复文件不存在: {fix_path}")
    rows = _read_csv(fix_path, "修复文件")
    if not rows:
        raise LedgerError(f"修复文件内容为空: {fix_path}")
    header = rows[0]
    data_rows = rows[1:]
    fix_index = _find_fix_column(header)
    if fix_index is None:
        raise LedgerError(f"修复文件缺少 fix_result 列: {fix_path}")
    original_header = [
        column for index, column in enumerate(header) if index != fix_index
    ]

    mapping, required = _load_mapping(conn, source)
    for column in original_header:
        if column not in mapping:
            raise LedgerError(f"修复文件含未映射目标列: {column}")
    mapped_targets = set(mapping.values())
    for field in required:
        if field not in mapped_targets:
            raise LedgerError(f"必需字段未全部映射: {field}")

    ok_rows: list[list[str]] = []
    rejected_rows: list[list[str]] = []
    for line, raw in enumerate(data_rows, start=2):
        if len(raw) != len(header):
            raise LedgerError(f"修复行列数与表头不一致（第 {line} 行）")
        verdict = raw[fix_index]
        original = [value for index, value in enumerate(raw) if index != fix_index]
        if verdict == "ok":
            ok_rows.append(original)
        elif verdict == "rejected":
            rejected_rows.append(original)
        else:
            raise LedgerError(f"fix_result 只接受小写 ok 或 rejected: {verdict!r}")

    # ok 行重新行校验：列数或必需字段仍不过关则整体失败、无任何变更。
    for index, raw in enumerate(ok_rows, start=1):
        if len(raw) != len(original_header):
            raise LedgerError(f"ok 行列数与表头不一致（第 {index} 条 ok 行）")
        record = {
            mapping[column]: value
            for column, value in zip(original_header, raw)
        }
        if any(not record.get(field) for field in required):
            raise LedgerError(f"ok 行仍缺少非空必需字段（第 {index} 条 ok 行）")

    # 判重与该来源全部历史已导入签名比较；同批修复行内也依次判重。
    seen = {
        record[0]
        for record in conn.execute(
            "SELECT signature FROM imported_records WHERE source_name = ?",
            (source,),
        )
    }
    new_signatures: list[str] = []
    duplicates = 0
    for raw in ok_rows:
        record = {
            mapping[column]: value
            for column, value in zip(original_header, raw)
        }
        signature = _row_signature(record)
        if signature in seen:
            duplicates += 1
            continue
        seen.add(signature)
        new_signatures.append(signature)
    fixed = len(new_signatures)

    counters = conn.execute(
        "SELECT imported_rows, duplicate_rows, rejected_rows,"
        " incremental_rows FROM import_batches"
        " WHERE source_name = ? AND batch_no = ?",
        (source, batch_no),
    ).fetchone()
    imported_rows, duplicate_rows, rejected_count, incremental_rows = counters
    processed = len(ok_rows) + len(rejected_rows)

    # rejected 行先暂存到临时文件，数据库提交成功后再原子替换复核文件；
    # 任一步失败都不会留下半应用状态。
    tmp_path: Path | None = None
    if rejected_rows:
        tmp_path = fix_path.with_name(fix_path.name + ".tmp")
        with tmp_path.open("w", encoding="utf-8", newline="") as fh:
            writer = csv.writer(fh)
            writer.writerow(original_header)
            writer.writerows(rejected_rows)

    try:
        with conn:  # 批次计数与新签名单次原子提交，异常即回滚
            conn.execute(
                "UPDATE import_batches SET imported_rows = ?, duplicate_rows = ?,"
                " rejected_rows = ?, incremental_rows = ?"
                " WHERE source_name = ? AND batch_no = ?",
                (
                    imported_rows + fixed,
                    duplicate_rows + duplicates,
                    rejected_count - processed,
                    incremental_rows + fixed,
                    source,
                    batch_no,
                ),
            )
            conn.executemany(
                "INSERT INTO imported_records (source_name, signature, batch_no)"
                " VALUES (?, ?, ?)",
                [(source, signature, batch_no) for signature in new_signatures],
            )
            conn.execute(
                "DELETE FROM fix_reviews WHERE source_name = ? AND batch_no = ?",
                (source, batch_no),
            )
    except BaseException:
        if tmp_path is not None:
            tmp_path.unlink(missing_ok=True)
        raise

    if tmp_path is not None:
        # 复核文件改写为 fix 结果文件（表头与原 CSV 一致、按修复文件顺序）。
        tmp_path.replace(fix_path)
    else:
        # 无 rejected 行：不生成 fix 结果文件，复核文件删除。
        fix_path.unlink()

    return ApplyFixedResult(
        source=source,
        batch_no=batch_no,
        fixed_rows=fixed,
        duplicate_rows=duplicates,
        rejected_rows=len(rejected_rows),
    )
