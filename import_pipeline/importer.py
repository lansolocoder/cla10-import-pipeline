"""批次导入执行：读取来源 CSV、按字段映射逐行导入并记录结果。

流程：
1. 预检（来源已注册、CSV 可读、表头列均有映射、必需字段全部映射），任一不满足
   即抛 LedgerError：退出码 1，不产生批次记录、拒绝文件或任何数据库变更。
2. 逐行处理：列数与表头不一致、必需字段缺失或为空的行隔离到
   rejected_<source>_<batch>.csv（字段顺序与原 CSV 一致，按文件内出现顺序）。
3. 通过校验的行按目标列名（字典序升序）拼接原值生成 sha256 签名，与此前任意
   批次已导入成功的记录比对，签名相同判为重复丢弃，否则导入并计为增量新数据。
"""

from __future__ import annotations

import csv
import hashlib
import sqlite3
from pathlib import Path

from . import ledger


def _resolve_csv_path(csv_path: str) -> Path:
    path = Path(csv_path)
    if not path.is_absolute():
        path = Path.cwd() / path
    return path


def _signature(values: dict[str, str]) -> str:
    """sha256(目标字段名=原值 以 & 连接)，目标字段名按字典序升序。"""
    payload = "&".join(f"{name}={values[name]}" for name in sorted(values))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def run_import(conn: sqlite3.Connection, source: str) -> dict[str, object]:
    """执行一个批次的导入，返回批次号与各项计数。预检失败抛 LedgerError。"""
    # ---- 预检阶段：任何一项失败都不得产生副作用 ----
    csv_path, required_fields, mappings = ledger.get_source_config(conn, source)

    path = _resolve_csv_path(csv_path)
    if not path.is_file():
        raise ledger.LedgerError(f"CSV 不存在或无法读取: {csv_path}")
    try:
        with path.open("r", encoding="utf-8", newline="") as handle:
            rows = list(csv.reader(handle))
    except (OSError, UnicodeError) as exc:
        raise ledger.LedgerError(f"CSV 不存在或无法读取: {csv_path}: {exc}") from exc

    # 必需字段（目标列名）必须全部存在映射
    mapped_targets = set(mappings.values())
    missing_required = [f for f in required_fields if f not in mapped_targets]
    if missing_required:
        raise ledger.LedgerError(
            "必需字段未全部映射: " + ",".join(missing_required)
        )

    header: list[str] | None = rows[0] if rows else None
    data_rows = rows[1:] if header is not None else []

    # 表头中的每个源列都必须有对应的目标列映射
    if header is not None:
        unmapped = [col for col in header if col not in mappings]
        if unmapped:
            raise ledger.LedgerError(
                "CSV 含未映射目标列: " + ",".join(unmapped)
            )

    # ---- 逐行处理 ----
    # 仅与此批次之前任意批次已成功导入的记录比对
    prior_signatures = ledger.known_signatures(conn, source)
    imported_signatures: list[str] = []
    rejected_rows: list[list[str]] = []
    duplicate_count = 0

    for row in data_rows:
        # 缺少表头时无法按表头取值；列数不一致同样隔离
        if header is None or len(row) != len(header):
            rejected_rows.append(row)
            continue
        row_values = dict(zip(header, row))
        # 按映射注册顺序组装目标列原值，不做 trim 或大小写转换，空值原样保留
        target_values: dict[str, str] = {}
        for source_column, target_column in mappings.items():
            if source_column in row_values:
                target_values[target_column] = row_values[source_column]
        # 必需字段须存在且值非空
        if any(not target_values.get(field) for field in required_fields):
            rejected_rows.append(row)
            continue
        signature = _signature(target_values)
        if signature in prior_signatures:
            duplicate_count += 1
            continue
        imported_signatures.append(signature)

    total_count = len(data_rows)
    failed_count = len(rejected_rows)
    success_count = len(imported_signatures)
    stats = {
        "total": total_count,
        "success": success_count,
        "duplicate": duplicate_count,
        "failed": failed_count,
        "new": success_count,  # 签名不同而导入的行即增量新数据
    }

    batch_no = ledger.next_batch_no(conn, source)

    # 失败行隔离（字段顺序与原 CSV 一致，按文件内出现顺序写出）
    rejected_path: Path | None = None
    if rejected_rows:
        rejected_path = Path.cwd() / f"rejected_{source}_{batch_no}.csv"
        try:
            with rejected_path.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.writer(handle)
                if header is not None:
                    writer.writerow(header)
                writer.writerows(rejected_rows)
        except OSError as exc:
            raise ledger.LedgerError(
                f"拒绝文件无法写入: {rejected_path}: {exc}"
            ) from exc

    try:
        ledger.save_batch(
            conn,
            source,
            batch_no,
            [(idx + 1, sig) for idx, sig in enumerate(imported_signatures)],
            stats,
        )
    except sqlite3.Error:
        # 落库失败时回滚已产生的拒绝文件，保持“失败不落任何变更”
        if rejected_path is not None:
            rejected_path.unlink(missing_ok=True)
        raise

    return {
        "source": source,
        "batch_no": batch_no,
        "rejected_path": rejected_path,
        **stats,
    }
