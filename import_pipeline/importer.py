"""导入执行：读取来源 CSV、按字段映射校验并整批落库或整批拒绝。"""

from __future__ import annotations

import csv
import sqlite3
from pathlib import Path

from . import ledger


def _reject(
    conn: sqlite3.Connection,
    source: str,
    reason: str,
    data_row_count: int,
) -> None:
    """登记 rejected 批次（成功 0 行、数据行全部隔离，不落任何数据）后报错。"""
    ledger.record_batch(
        conn,
        source,
        "rejected",
        success_count=0,
        quarantined_count=data_row_count,
    )
    raise ledger.LedgerError(reason)


def run_import(conn: sqlite3.Connection, source: str) -> int:
    """执行一次导入，返回成功行数。

    文件不存在时直接报错且不产生批次记录；校验未通过时登记 rejected
    批次并报错；全部通过时数据原子落库并登记 ok 批次。
    """
    csv_path, required_fields = ledger.get_source(conn, source)
    path = Path(csv_path)
    if not path.is_file():
        raise ledger.LedgerError(f"CSV 文件不存在: {csv_path}")

    source_to_target = dict(ledger.list_mappings(conn, source))

    try:
        with path.open(newline="", encoding="utf-8") as fh:
            raw_rows = list(csv.reader(fh))
    except OSError as exc:
        raise ledger.LedgerError(f"CSV 文件无法读取: {csv_path}") from exc

    if not raw_rows:
        # 无表头可校验：按整批拒绝登记，不产生数据。
        ledger.record_batch(conn, source, "rejected", 0, 0)
        raise ledger.LedgerError("CSV 缺少表头")

    header = raw_rows[0]
    data_rows = [row for row in raw_rows[1:] if row]  # 跳过末尾空行
    data_row_count = len(data_rows)

    unmapped = [column for column in header if column not in source_to_target]
    if unmapped:
        _reject(
            conn, source, f"存在未映射的源列: {','.join(unmapped)}", data_row_count
        )

    covered_targets = set(source_to_target.values())
    missing_targets = [
        field for field in required_fields if field not in covered_targets
    ]
    if missing_targets:
        _reject(
            conn,
            source,
            f"缺少映射目标对应列: {','.join(missing_targets)}",
            data_row_count,
        )

    index = {column: pos for pos, column in enumerate(header)}
    records: list[dict[str, str]] = []
    for line_no, row in enumerate(data_rows, start=2):
        record: dict[str, str] = {}
        for source_column in header:
            pos = index[source_column]
            value = row[pos] if pos < len(row) else ""
            if not value.strip():
                _reject(
                    conn,
                    source,
                    f"第 {line_no} 行存在空白值",
                    data_row_count,
                )
            record[source_to_target[source_column]] = value
        records.append(record)

    ledger.record_batch(
        conn,
        source,
        "ok",
        success_count=len(records),
        quarantined_count=0,
        records=records,
    )
    return len(records)
