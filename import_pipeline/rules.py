"""导入校验规则：decimal/date/enum 三种类型的取值判断。

注册时的参数解析在 ledger.add_rule 中完成；本模块提供注册与导入
共用的取值判断，保证两处语义一致。
"""

from __future__ import annotations

import re
from datetime import date
from decimal import Decimal, InvalidOperation

RULE_TYPES = ("decimal", "date", "enum")

# 十进制数值：可选正负号，数字与至多一个小数点，不允许空格等其他符号。
_DECIMAL_RE = re.compile(r"[+-]?([0-9]+(\.[0-9]*)?|\.[0-9]+)")
# ISO 日期：严格的 YYYY-MM-DD 形式。
_DATE_RE = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}")


def parse_decimal(value: str) -> Decimal | None:
    """解析十进制数值；非法写法（含空格、多余符号、多个小数点）返回 None。"""
    if not _DECIMAL_RE.fullmatch(value):
        return None
    try:
        return Decimal(value)
    except InvalidOperation:
        return None


def is_iso_date(value: str) -> bool:
    """严格的 YYYY-MM-DD 且日期真实存在（2024-02-29 合法、2023-02-29 非法）。"""
    if not _DATE_RE.fullmatch(value):
        return False
    year, month, day = (int(part) for part in value.split("-"))
    try:
        date(year, month, day)
    except ValueError:
        return False
    return True


def rule_accepts(
    rule_type: str,
    value: str,
    min_value: str | None,
    max_value: str | None,
    candidates: str | None,
) -> bool:
    """按规则类型判断字段值是否通过；边界值本身通过，按数值比较。"""
    if rule_type == "decimal":
        number = parse_decimal(value)
        if number is None:
            return False
        if min_value is not None and number < Decimal(min_value):
            return False
        if max_value is not None and number > Decimal(max_value):
            return False
        return True
    if rule_type == "date":
        return is_iso_date(value)
    if rule_type == "enum":
        return value in (candidates or "").split(",")
    raise ValueError(f"未知规则类型: {rule_type}")
