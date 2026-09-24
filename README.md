# 数据导入与校验台账

用于本地来源数据导入与校验记录管理的命令行项目。

需要 Python 3.12，无第三方依赖。在仓库根目录运行：

```bash
python3 -m import_pipeline --help
python3 -m import_pipeline --version
python3 -m unittest discover -s tests -v
```

无参数显示帮助，未知参数以非零状态退出。

## 配置子命令

所有配置持久化到仓库根目录的 `import_ledger.db`（SQLite，标准库管理），这是唯一的持久化文件。成功操作在 stdout 打印一行 `Result: <操作> <来源名> <条目数>`；业务规则拒绝（如重名、冲突）退出码为 1 且不落库，用法错误（未知子命令、缺失参数）按 argparse 约定退出码为 2，错误信息均写 stderr。

```bash
# 注册来源配置：来源名（唯一）、CSV 路径、至少一个必需字段名（非空、不重复）
python3 -m import_pipeline add-source orders data/orders.csv id amount

# 注册字段映射：来源名、源列名、目标列名；同一来源下源列名唯一
python3 -m import_pipeline add-mapping orders order_id id

# 只读查询：列出全部来源（来源名、路径、逗号连接的字段列表，制表符分隔）
python3 -m import_pipeline list-sources

# 只读查询：列出某来源的字段映射（源列名、目标列名，制表符分隔）
python3 -m import_pipeline list-mappings orders
```

校验规则：来源名、路径、字段名、列名均须为非空字符串；布尔与状态字面值只接受小写 `true`/`false`/`ok`/`failed`/`rejected`，大小写不同（如 `TRUE`）视为非法输入并拒绝。重复注册来源名、重复字段名、同一来源下重复源列名、引用不存在的来源均被拒绝，且数据库保持执行前状态（单次操作原子提交）。

尚未实现增量导入、校验失败隔离以及重跑与结果台账。
