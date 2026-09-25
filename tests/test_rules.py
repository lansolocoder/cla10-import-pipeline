"""Checks for validation rule registration, listing and import enforcement."""

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
        for source_column, target_column in [
            ("order_id", "id"),
            ("total", "amount"),
            ("created", "day"),
            ("state", "status"),
        ]:
            self.assertEqual(
                self.invoke(
                    "add-mapping", "orders", source_column, target_column
                ).returncode,
                0,
            )

    def rules_listing(self, source: str = "orders") -> list[str]:
        result = self.invoke("rules", source)
        self.assertEqual(result.returncode, 0, result.stderr)
        return result.stdout.splitlines()

    def test_add_rule_and_rules_listing(self) -> None:
        csv_path = self.write_csv("orders.csv", "order_id,total,created,state\n")
        self.register_orders(csv_path)

        result = self.invoke("add-rule", "orders", "amount", "decimal", "0~100")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(
            result.stdout.strip(), "Result: add-rule orders amount decimal 1"
        )
        self.assertEqual(result.stderr, "")
        self.assertEqual(
            self.invoke("add-rule", "orders", "day", "date").returncode, 0
        )
        self.assertEqual(
            self.invoke("add-rule", "orders", "status", "enum", "new,paid").returncode,
            0,
        )
        self.assertEqual(
            self.invoke("add-rule", "orders", "id", "decimal", "~10").returncode, 0
        )
        self.assertEqual(
            self.invoke("add-rule", "orders", "amount", "enum", "x").returncode, 0
        )

        self.assertEqual(
            self.rules_listing(),
            [
                "amount\tdecimal\t0~100",
                "amount\tenum\tx",
                "day\tdate\t-",
                "id\tdecimal\t~10",
                "status\tenum\tnew,paid",
                "Result: rules orders",
            ],
        )

    def test_rules_empty_outputs_only_result_line(self) -> None:
        csv_path = self.write_csv("orders.csv", "order_id,total,created,state\n")
        self.register_orders(csv_path)
        self.assertEqual(self.rules_listing(), ["Result: rules orders"])

    def test_rules_unknown_source_is_error(self) -> None:
        result = self.invoke("rules", "missing")
        self.assertEqual(result.returncode, 1)
        self.assertIn("missing", result.stderr)
        self.assertEqual(result.stdout, "")

    def test_add_rule_decimal_without_bounds(self) -> None:
        csv_path = self.write_csv("orders.csv", "order_id,total,created,state\n")
        self.register_orders(csv_path)
        self.assertEqual(
            self.invoke("add-rule", "orders", "amount", "decimal").returncode, 0
        )
        self.assertEqual(
            self.rules_listing(),
            ["amount\tdecimal\t-", "Result: rules orders"],
        )

    def test_add_rule_rejections_keep_existing_rules(self) -> None:
        csv_path = self.write_csv("orders.csv", "order_id,total,created,state\n")
        self.register_orders(csv_path)
        self.assertEqual(
            self.invoke("add-rule", "orders", "amount", "decimal", "1~5").returncode,
            0,
        )
        before = self.rules_listing()

        cases = [
            ("add-rule", "missing", "amount", "decimal"),  # 来源不存在
            ("add-rule", "orders", "unknown", "decimal"),  # 非必需字段
            ("add-rule", "orders", "amount", "text"),  # 未知类型
            ("add-rule", "orders", "amount", "DECIMAL"),  # 类型大小写
            ("add-rule", "orders", "day", "decimal", "abc~5"),  # 边界非数值
            ("add-rule", "orders", "day", "decimal", "5~1"),  # 最小值大于最大值
            ("add-rule", "orders", "day", "decimal", "~"),  # 未指定边界
            ("add-rule", "orders", "day", "decimal", "5"),  # 边界缺少分隔符
            ("add-rule", "orders", "day", "date", "1~2"),  # date 不接受参数
            ("add-rule", "orders", "status", "enum"),  # 未指定候选值
            ("add-rule", "orders", "status", "enum", "a,,b"),  # 候选值含空项
            ("add-rule", "orders", "status", "enum", "a,"),  # 候选值含空项
            ("add-rule", "orders", "status", "enum", "a,b,a"),  # 候选值重复
            ("add-rule", "orders", "amount", "decimal", "0~9"),  # 同字段同类型重复
        ]
        for arguments in cases:
            with self.subTest(arguments=arguments):
                result = self.invoke(*arguments)
                self.assertEqual(result.returncode, 1, result.stdout)
                self.assertNotEqual(result.stderr, "")
                self.assertNotIn("Result:", result.stdout)
        self.assertEqual(self.rules_listing(), before)

    def test_add_rule_missing_arguments_are_usage_errors(self) -> None:
        result = self.invoke("add-rule", "orders", "amount")
        self.assertEqual(result.returncode, 2)
        self.assertNotEqual(result.stderr, "")
        self.assertEqual(result.stdout, "")

    def test_import_enforces_registered_rules(self) -> None:
        csv_path = self.write_csv(
            "orders.csv",
            "order_id,total,created,state\n"
            "A1,10,2024-02-29,new\n"  # 合法（闰日）
            "A2,10.5.1,2024-01-01,new\n"  # decimal 非法
            "A3,-5,2024-01-01,new\n"  # 低于最小值
            "A4,200,2024-01-01,new\n"  # 高于最大值
            "A5,50,2023-02-29,new\n"  # 日期不存在
            "A6,50,2024-1-1,new\n"  # 日期格式非法
            "A7,50,2024-01-01,NEW\n"  # enum 不折叠大小写
            "A8,0,2024-01-01,paid\n"  # 边界值本身通过
            "A9,100,2024-01-01,paid\n"  # 边界值本身通过
            "A10,,2024-01-01,new\n",  # 必需字段为空
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
            self.invoke("add-rule", "orders", "status", "enum", "new,paid").returncode,
            0,
        )

        result = self.invoke("import", "orders")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(
            result.stdout.strip(), "Result: import orders 1 10 3 0 7 3"
        )
        rejected = (self.work / "rejected_orders_1.csv").read_text(
            encoding="utf-8"
        ).splitlines()
        self.assertEqual(
            rejected,
            [
                "order_id,total,created,state",
                "A2,10.5.1,2024-01-01,new",
                "A3,-5,2024-01-01,new",
                "A4,200,2024-01-01,new",
                "A5,50,2023-02-29,new",
                "A6,50,2024-1-1,new",
                "A7,50,2024-01-01,NEW",
                "A10,,2024-01-01,new",
            ],
        )

    def test_reimport_enforces_registered_rules(self) -> None:
        csv_path = self.write_csv(
            "orders.csv", "order_id,total,created,state\nA1,10,2024-01-01,new\n"
        )
        self.register_orders(csv_path)
        self.assertEqual(self.invoke("import", "orders").returncode, 0)

        self.assertEqual(
            self.invoke("add-rule", "orders", "amount", "decimal", "0~5").returncode,
            0,
        )
        result = self.invoke("reimport", "orders", "1")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "Result: reimport orders 1 1 0 0 1 0")
        self.assertTrue((self.work / "rejected_orders_1.csv").is_file())
        self.assertEqual(
            self.invoke("batches", "orders").stdout.splitlines(),
            ["1\t1\t0\t0\t1\t0", "Result: batches orders"],
        )

    def test_import_without_rules_behaves_as_before(self) -> None:
        csv_path = self.write_csv(
            "orders.csv", "order_id,total,created,state\nA1,abc,not-a-date,X\n"
        )
        self.register_orders(csv_path)
        result = self.invoke("import", "orders")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "Result: import orders 1 1 1 0 0 1")


if __name__ == "__main__":
    unittest.main()
