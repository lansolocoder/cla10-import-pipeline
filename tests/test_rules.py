"""Checks for validation rule registration and import-time enforcement."""

from pathlib import Path
import os
import subprocess
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]


class RuleCommandTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.work = Path(self.tmp.name)
        self.db_path = self.work / "import_ledger.db"

    def invoke(self, *arguments: str) -> subprocess.CompletedProcess[str]:
        env = dict(
            os.environ,
            IMPORT_LEDGER_DB=str(self.db_path),
            PYTHONPATH=str(ROOT),
        )
        return subprocess.run(
            [sys.executable, "-m", "import_pipeline", *arguments],
            cwd=self.work,
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )

    def write_csv(self, name: str, content: str) -> str:
        path = self.work / name
        path.write_text(content, encoding="utf-8")
        return str(path)

    def register_orders(self, csv_path: str) -> None:
        self.assertEqual(
            self.invoke(
                "add-source", "orders", csv_path, "id", "amount", "day", "status"
            ).returncode,
            0,
        )
        for column, target in (
            ("order_id", "id"),
            ("total", "amount"),
            ("order_day", "day"),
            ("state", "status"),
        ):
            self.assertEqual(
                self.invoke("add-mapping", "orders", column, target).returncode, 0
            )

    def test_add_rule_and_rules_listing(self) -> None:
        csv_path = self.write_csv("orders.csv", "order_id,total,order_day,state\n")
        self.register_orders(csv_path)

        result = self.invoke("add-rule", "orders", "amount", "decimal", "0~100")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(
            result.stdout.strip(), "Result: add-rule orders amount decimal 1"
        )
        self.assertEqual(
            self.invoke("add-rule", "orders", "amount", "enum", "big,small").returncode,
            0,
        )
        self.assertEqual(
            self.invoke("add-rule", "orders", "day", "date").returncode, 0
        )
        self.assertEqual(
            self.invoke("add-rule", "orders", "id", "decimal", "10~").returncode, 0
        )
        self.assertEqual(
            self.invoke("add-rule", "orders", "status", "decimal", "~5").returncode, 0
        )

        result = self.invoke("rules", "orders")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(
            result.stdout.splitlines(),
            [
                "amount\tdecimal\t0~100",
                "amount\tenum\tbig,small",
                "day\tdate\t-",
                "id\tdecimal\t10~",
                "status\tdecimal\t~5",
                "Result: rules orders",
            ],
        )
        self.assertEqual(result.stderr, "")

    def test_rules_empty_outputs_only_result_line(self) -> None:
        csv_path = self.write_csv("orders.csv", "order_id,total,order_day,state\n")
        self.register_orders(csv_path)
        result = self.invoke("rules", "orders")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "Result: rules orders")

    def test_rules_unknown_source_is_error(self) -> None:
        result = self.invoke("rules", "missing")
        self.assertEqual(result.returncode, 1)
        self.assertIn("missing", result.stderr)
        self.assertEqual(result.stdout, "")

    def test_add_rule_rejections_keep_existing_rules(self) -> None:
        csv_path = self.write_csv("orders.csv", "order_id,total,order_day,state\n")
        self.register_orders(csv_path)
        self.assertEqual(
            self.invoke("add-rule", "orders", "amount", "decimal", "0~100").returncode,
            0,
        )

        rejections = [
            ("add-rule", "missing", "amount", "decimal", "0~1"),  # 来源不存在
            ("add-rule", "orders", "nope", "decimal", "0~1"),  # 非必需字段
            ("add-rule", "orders", "amount", "bool"),  # 未知类型
            ("add-rule", "orders", "amount", "decimal", "x~1"),  # 边界非数值
            ("add-rule", "orders", "amount", "decimal", "1~x"),  # 边界非数值
            ("add-rule", "orders", "amount", "decimal", "100~0"),  # 最小大于最大
            ("add-rule", "orders", "amount", "decimal", "0~100"),  # 重复规则
            ("add-rule", "orders", "status", "enum"),  # 未指定候选值
            ("add-rule", "orders", "status", "enum", "a,,b"),  # 候选值含空项
            ("add-rule", "orders", "status", "enum", "a,b,"),  # 候选值含空项
            ("add-rule", "orders", "status", "enum", "a,a"),  # 候选值重复
            ("add-rule", "orders", "day", "date", "extra"),  # date 不接受参数
        ]
        for arguments in rejections:
            with self.subTest(arguments=arguments):
                result = self.invoke(*arguments)
                self.assertEqual(result.returncode, 1, result.stderr)
                self.assertNotEqual(result.stderr, "")
                self.assertNotIn("Result:", result.stdout)

        result = self.invoke("rules", "orders")
        self.assertEqual(
            result.stdout.splitlines(),
            ["amount\tdecimal\t0~100", "Result: rules orders"],
        )

    def test_add_rule_decimal_without_bounds(self) -> None:
        csv_path = self.write_csv("orders.csv", "order_id,total,order_day,state\n")
        self.register_orders(csv_path)
        result = self.invoke("add-rule", "orders", "amount", "decimal")
        self.assertEqual(result.returncode, 0, result.stderr)
        result = self.invoke("rules", "orders")
        self.assertEqual(
            result.stdout.splitlines(),
            ["amount\tdecimal\t-", "Result: rules orders"],
        )

    def test_import_enforces_decimal_date_and_enum_rules(self) -> None:
        csv_path = self.write_csv(
            "orders.csv",
            "order_id,total,order_day,state\n"
            "A1,10,2024-02-29,paid\n"   # 全部通过（闰日合法）
            "A2,1 0,2024-01-01,paid\n"  # decimal 含空格
            "A3,1.2.3,2024-01-01,paid\n"  # decimal 多个小数点
            "A4,150,2024-01-01,paid\n"  # decimal 超出最大值
            "A5,-1,2024-01-01,paid\n"   # decimal 低于最小值
            "A6,10,2023-02-29,paid\n"   # 不存在的日期
            "A7,10,2024-2-9,paid\n"     # 非 YYYY-MM-DD 格式
            "A8,10,2024-01-01,Paid\n",  # enum 不做大小写折叠
        )
        self.register_orders(csv_path)
        self.assertEqual(
            self.invoke("add-rule", "orders", "amount", "decimal", "0~100").returncode,
            0,
        )
        self.assertEqual(
            self.invoke("add-rule", "orders", "day", "date").returncode, 0
        )
        self.assertEqual(
            self.invoke("add-rule", "orders", "status", "enum", "paid,open").returncode,
            0,
        )

        result = self.invoke("import", "orders")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "Result: import orders 1 8 1 0 7 1")

        rejected = (self.work / "rejected_orders_1.csv").read_text(encoding="utf-8")
        self.assertEqual(
            rejected.splitlines(),
            [
                "order_id,total,order_day,state",
                "A2,1 0,2024-01-01,paid",
                "A3,1.2.3,2024-01-01,paid",
                "A4,150,2024-01-01,paid",
                "A5,-1,2024-01-01,paid",
                "A6,10,2023-02-29,paid",
                "A7,10,2024-2-9,paid",
                "A8,10,2024-01-01,Paid",
            ],
        )

    def test_decimal_boundary_values_pass(self) -> None:
        csv_path = self.write_csv(
            "orders.csv",
            "order_id,total,order_day,state\nA1,0,2024-01-01,paid\nA2,100,2024-01-01,paid\n",
        )
        self.register_orders(csv_path)
        self.assertEqual(
            self.invoke("add-rule", "orders", "amount", "decimal", "0~100").returncode,
            0,
        )
        result = self.invoke("import", "orders")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "Result: import orders 1 2 2 0 0 2")

    def test_reimport_applies_rules(self) -> None:
        csv_path = self.write_csv(
            "orders.csv", "order_id,total,order_day,state\nA1,10,2024-01-01,paid\n"
        )
        self.register_orders(csv_path)
        self.assertEqual(self.invoke("import", "orders").returncode, 0)
        # 导入后注册规则，重跑时原行不再通过。
        self.assertEqual(
            self.invoke("add-rule", "orders", "amount", "decimal", "50~100").returncode,
            0,
        )
        result = self.invoke("reimport", "orders", "1")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "Result: reimport orders 1 1 0 0 1 0")
        rejected = (self.work / "rejected_orders_1.csv").read_text(encoding="utf-8")
        self.assertEqual(
            rejected.splitlines(),
            ["order_id,total,order_day,state", "A1,10,2024-01-01,paid"],
        )

    def test_source_without_rules_imports_as_before(self) -> None:
        csv_path = self.write_csv(
            "orders.csv",
            "order_id,total,order_day,state\nA1,xyz,not-a-date,UNKNOWN\n",
        )
        self.register_orders(csv_path)
        result = self.invoke("import", "orders")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "Result: import orders 1 1 1 0 0 1")
        self.assertFalse((self.work / "rejected_orders_1.csv").exists())


if __name__ == "__main__":
    unittest.main()
