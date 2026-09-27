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


def connect_readonly() -> sqlite3.Connection:
    """只读连接：以 mode=ro 打开台账，任何路径都不会写入数据库文件。

    台账文件不存在时返回带空表的内存连接，查询结果等同于空台账。
    """
    path = db_path()
    if path.is_file():
        return sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    conn = sqlite3.connect(":memory:")
    conn.executescript(_SCHEMA)
    return conn


def _validate_token(value: str, label: str) -> None:
    if not value or not value.strip():
        raise LedgerError(f"{label}不能为空")
    if value.lower() in RESERVED_LITERALS and value not in RESERVED_LITERALS:
        raise LedgerError(f"{label}含非法字面值: {value}")


def _validate_batch_no(batch_no: int) -> None:
    if not isinstance(batch_no, int) or isinstance(batch_no, bool) or batch_no <= 0:
        raise LedgerError(f"批次号必须为正整数: {batch_no}")


def _parse_positive_int(value: object, label: str) -> int:
    """把命令行传入的批次号解析为正整数；非十进制正整数按业务规则拒绝。"""
    if isinstance(value, bool):
        raise LedgerError(f"{label}必须为正整数: {value}")
    if isinstance(value, int):
        number = value
    elif isinstance(value, str) and value.isascii() and value.isdigit():
        number = int(value)
    else:
        raise LedgerError(f"{label}必须为正整数: {value}")
    if number <= 0:
        raise LedgerError(f"{label}必须为正整数: {value}")
    return number


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


def run_import(conn: sqlite3.Connection, source: str) -> int:
    """执行一次导入，返回成功行数。

    文件不存在时不留批次记录；校验失败登记 rejected 批次后抛 LedgerError；
    成功时数据行与 ok 批次在同一事务内落库。
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

    error: str | None = None
    if header is None:
        error = "CSV 文件缺少表头"
    else:
        for column in header:
            if column not in target_by_source:
                error = f"未映射的源列: {column}"
                break
        if error is None:
            produced = {target_by_source[column] for column in header}
            for field in required_fields:
                if field not in produced:
                    error = f"缺少映射目标对应列: {field}"
                    break
        if error is None:
            for data_row in data_rows:
                if len(data_row) != len(header) or any(
                    not value.strip() for value in data_row
                ):
                    error = "数据行含空白值"
                    break

    if error is not None:
        with conn:  # 拒绝也留批次记录；已校验行不落库
            _record_batch(conn, source, "rejected", 0, len(data_rows))
        raise LedgerError(error)

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


def revoke_batch(conn: sqlite3.Connection, source: str, batch_no: int) -> None:
    """撤销某来源指定批次号的单个 ok 批次。

    批次状态置为 revoked，该批次的 imported_rows 全部删除；批次计数保持不变。
    仅允许撤销 ok 批次；来源/批次不存在或状态非 ok 时拒绝且不留任何变更。
    """
    _validate_token(source, "来源名")
    _validate_batch_no(batch_no)
    if not conn.execute(
        "SELECT 1 FROM sources WHERE name = ?", (source,)
    ).fetchone():
        raise LedgerError(f"来源不存在: {source}")
    row = conn.execute(
        "SELECT status FROM batches WHERE source_name = ? AND batch_no = ?",
        (source, batch_no),
    ).fetchone()
    if row is None:
        raise LedgerError(f"批次不存在: {batch_no}")
    if row[0] != "ok":
        raise LedgerError(f"批次状态不允许撤销: {row[0]}")
    try:
        with conn:  # 删除行与状态翻转原子提交，异常即整体回滚
            conn.execute(
                "DELETE FROM imported_rows"
                " WHERE source_name = ? AND batch_no = ?",
                (source, batch_no),
            )
            conn.execute(
                "UPDATE batches SET status = 'revoked'"
                " WHERE source_name = ? AND batch_no = ?",
                (source, batch_no),
            )
    except sqlite3.Error as exc:
        raise LedgerError(f"批次撤销失败: {exc}") from exc


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


def batch_stats(
    conn: sqlite3.Connection, source: str, start_batch: str, end_batch: str
) -> tuple[int, int, int, int, int, int]:
    """汇总某来源批次号闭区间 [start_batch, end_batch] 内各状态的批次统计。

    返回 (批次总数, ok 批次数, rejected 批次数, revoked 批次数,
    成功行合计, 被隔离行合计)。区间内没有任何批次时返回全零元组。
    revoked 批次不计入 ok 批次数，但其成功行数与被隔离行数按撤销前保留值
    计入两个行数合计。来源不存在、来源名非法、批次号非正整数或
    起始批次号大于结束批次号时拒绝。只读操作，不落库。
    """
    _validate_token(source, "来源名")
    start_no = _parse_positive_int(start_batch, "起始批次号")
    end_no = _parse_positive_int(end_batch, "结束批次号")
    if start_no > end_no:
        raise LedgerError(
            f"起始批次号不能大于结束批次号: {start_no} > {end_no}"
        )
    if not conn.execute(
        "SELECT 1 FROM sources WHERE name = ?", (source,)
    ).fetchone():
        raise LedgerError(f"来源不存在: {source}")
    rows = conn.execute(
        "SELECT status, succeeded_rows, quarantined_rows FROM batches"
        " WHERE source_name = ? AND batch_no >= ? AND batch_no <= ?",
        (source, start_no, end_no),
    ).fetchall()
    total = len(rows)
    ok = rejected = revoked = succeeded_total = quarantined_total = 0
    for status, succeeded, quarantined in rows:
        if status == "ok":
            ok += 1
        elif status == "rejected":
            rejected += 1
        elif status == "revoked":
            revoked += 1
        succeeded_total += succeeded
        quarantined_total += quarantined
    return (
        total,
        ok,
        rejected,
        revoked,
        succeeded_total,
        quarantined_total,
    )


def batch_stats_detail(
    conn: sqlite3.Connection,
    source: str,
    start_batch: str,
    end_batch: str,
    status: str,
) -> list[tuple[int, int, int]]:
    """列出某来源批次号闭区间 [start_batch, end_batch] 内指定状态的批次明细。

    在一次只读扫描里返回 (批次号, 成功行数, 被隔离行数) 列表，按批次号升序。
    revoked 批次输出撤销时保留的撤销前数值；rejected 批次成功行数为 0。
    区间内没有该状态的批次时返回空列表。状态只接受小写 ok/rejected/revoked。
    来源不存在、来源名非法、批次号非正整数、起始批次号大于结束批次号或
    状态取值非法时拒绝。只读操作，不落库。
    """
    _validate_token(source, "来源名")
    start_no = _parse_positive_int(start_batch, "起始批次号")
    end_no = _parse_positive_int(end_batch, "结束批次号")
    if start_no > end_no:
        raise LedgerError(
            f"起始批次号不能大于结束批次号: {start_no} > {end_no}"
        )
    if status not in ("ok", "rejected", "revoked"):
        raise LedgerError(f"状态取值非法: {status}")
    if not conn.execute(
        "SELECT 1 FROM sources WHERE name = ?", (source,)
    ).fetchone():
        raise LedgerError(f"来源不存在: {source}")
    return conn.execute(
        "SELECT batch_no, succeeded_rows, quarantined_rows FROM batches"
        " WHERE source_name = ? AND batch_no >= ? AND batch_no <= ?"
        " AND status = ? ORDER BY batch_no",
        (source, start_no, end_no, status),
    ).fetchall()


def batch_report(
    conn: sqlite3.Connection,
    source: str,
    start_batch: str,
    end_batch: str,
) -> dict[str, tuple[int, int, int, list[tuple[int, int, int]]]]:
    """在一次只读扫描里给出闭区间 [start_batch, end_batch] 内三类状态的汇总与明细。

    返回以 ok/rejected/revoked 为键的字典；每项为
    (批次数, 成功行合计, 被隔离行合计, 明细列表)，明细列表元素为
    (批次号, 成功行数, 被隔离行数)，按批次号升序。三段数字来自同一次
    只读扫描的一致快照。revoked 批次按撤销前保留值输出并计入合计；
    rejected 批次成功行数为 0。区间内无批次时三段计数全 0、明细为空。
    来源不存在、来源名非法、批次号非正整数或起始批次号大于结束批次号时拒绝。
    只读操作，不落库。
    """
    _validate_token(source, "来源名")
    start_no = _parse_positive_int(start_batch, "起始批次号")
    end_no = _parse_positive_int(end_batch, "结束批次号")
    if start_no > end_no:
        raise LedgerError(
            f"起始批次号不能大于结束批次号: {start_no} > {end_no}"
        )
    if not conn.execute(
        "SELECT 1 FROM sources WHERE name = ?", (source,)
    ).fetchone():
        raise LedgerError(f"来源不存在: {source}")
    rows = conn.execute(
        "SELECT batch_no, status, succeeded_rows, quarantined_rows FROM batches"
        " WHERE source_name = ? AND batch_no >= ? AND batch_no <= ?"
        " ORDER BY batch_no",
        (source, start_no, end_no),
    ).fetchall()
    sections: dict[str, list] = {
        status: [0, 0, 0, []] for status in ("ok", "rejected", "revoked")
    }
    for batch_no, status, succeeded, quarantined in rows:
        section = sections.get(status)
        if section is None:
            continue
        section[0] += 1
        section[1] += succeeded
        section[2] += quarantined
        section[3].append((batch_no, succeeded, quarantined))
    return {
        status: (section[0], section[1], section[2], section[3])
        for status, section in sections.items()
    }


def show_rows(conn: sqlite3.Connection, source: str, batch_no: int) -> list[str]:
    """返回某来源指定批次的已导入行，每行为 "行号\\t规范化 JSON"。

    规范化 JSON：对象键按目标列名字典序排序，值均为字符串。仅 ok 批次有行；
    rejected 批次本无行、revoked 批次的行已删除，均返回空列表。
    来源名或批次号不存在时拒绝。只读操作，不落库。
    """
    _validate_token(source, "来源名")
    _validate_batch_no(batch_no)
    if not conn.execute(
        "SELECT 1 FROM sources WHERE name = ?", (source,)
    ).fetchone():
        raise LedgerError(f"来源不存在: {source}")
    row = conn.execute(
        "SELECT status FROM batches WHERE source_name = ? AND batch_no = ?",
        (source, batch_no),
    ).fetchone()
    if row is None:
        raise LedgerError(f"批次不存在: {batch_no}")
    if row[0] != "ok":
        return []
    lines = []
    for row_number, data in conn.execute(
        "SELECT row_number, data FROM imported_rows"
        " WHERE source_name = ? AND batch_no = ? ORDER BY row_number",
        (source, batch_no),
    ):
        normalized = json.dumps(
            json.loads(data), ensure_ascii=False, sort_keys=True
        )
        lines.append(f"{row_number}\t{normalized}")
    return lines


def find_dupes(
    conn: sqlite3.Connection, source: str, target_column: str
) -> list[tuple[str, int, list[int]]]:
    """按目标列值统计某来源未撤销批次已落库行的重复组。

    返回 (目标列值, 出现次数, 升序去重批次号列表)，按目标列值升序；
    仅保留出现两行及以上的组。已撤销批次的行不参与统计。
    来源不存在、来源名非法、目标列名为空或目标列不存在于任何已落库行时拒绝。
    只读操作，不落库。
    """
    _validate_token(source, "来源名")
    if not target_column or not target_column.strip():
        raise LedgerError("目标列名不能为空")
    if not conn.execute(
        "SELECT 1 FROM sources WHERE name = ?", (source,)
    ).fetchone():
        raise LedgerError(f"来源不存在: {source}")
    rows = conn.execute(
        "SELECT r.batch_no, r.data FROM imported_rows r"
        " JOIN batches b"
        " ON b.source_name = r.source_name AND b.batch_no = r.batch_no"
        " WHERE r.source_name = ? AND b.status != 'revoked'",
        (source,),
    ).fetchall()
    known_columns: set[str] = set()
    counts: dict[str, int] = {}
    batches_by_value: dict[str, set[int]] = {}
    for batch_no, data in rows:
        record = json.loads(data)
        known_columns.update(record)
        if target_column in record:
            value = record[target_column]
            counts[value] = counts.get(value, 0) + 1
            batches_by_value.setdefault(value, set()).add(batch_no)
    if target_column not in known_columns:
        raise LedgerError(f"目标列不存在: {target_column}")
    return [
        (value, counts[value], sorted(batches_by_value[value]))
        for value in sorted(counts)
        if counts[value] >= 2
    ]
