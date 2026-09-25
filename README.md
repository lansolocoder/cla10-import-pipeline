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

## 导入批次

```bash
# 执行一次导入：读取来源登记的 CSV 路径，按字段映射落库
python3 -m import_pipeline run-import orders

# 只读查询：输出来源自给定批次号起（含）的各批次，
# 每行：批次号、状态、成功行数、被隔离行数（制表符分隔，按批次号升序）
python3 -m import_pipeline show-batch orders 1
```

`run-import` 的 CSV 首行为表头，数据行按已登记的字段映射写入 `imported_rows`。出现未映射的源列、必需字段缺少映射目标对应列，或数据行的值为空白时整批拒绝：退出码 1、stderr 一行 `Error:`，批次状态记为 `rejected`，已校验行不落库；否则整批落库，退出码 0，stdout 输出 `Result: run-import <来源名> <成功行数>`，批次状态记为 `ok`。CSV 文件不存在时退出码 1 且不留批次记录。无论成功或拒绝都会在 `batches` 表留下批次记录：批次号（来源内从 1 递增）、来源名、状态、成功行数、被隔离行数。重复执行视为新批次，已有批次记录不变。`show-batch` 的来源名或批次号不存在时退出码 1、stderr 一行 `Error:`。

## 增量导入与隔离记录

```bash
# 执行一次增量导入：以来源声明的必需字段 id 为业务键去重
python3 -m import_pipeline delta-import orders

# 只读查询：列出来源全部隔离记录，每行：批次号、行号、原因、
# 按目标列名排序后逗号连接的 列名=值 对（制表符分隔，按批次号与行号升序）
python3 -m import_pipeline list-quarantine orders
```

`delta-import` 按来源配置声明的必需字段 `id`（目标列名）作为业务键，按字符串精确匹配，不做大小写或空白归一。数据行的业务键与此前任何批次已落库行相同的为重复行：不写入也不报错，计入跳过行数；业务键同时出现在本批多条数据行的，这些行全部为冲突行，逐行写入隔离记录（原因 `duplicate-key`）；值为空白的行同样逐行隔离（原因 `blank-value`），行号取该行在 CSV 数据行中的序号（从 1 开始）；其余行按字段映射写入数据表并计入成功行数。正常完成时退出码 0，stdout 输出 `Result: delta-import <来源名> <新增行数> <跳过行数> <被隔离行数>`，并登记一条新批次记录：存在被隔离行时状态为 `failed`，否则为 `ok`。整批级问题（表头含未映射源列、缺少必需字段对应列、来源未声明必需字段 `id`）整批拒绝：退出码 1、stderr 一行 `Error:`，批次状态记为 `rejected`，无任何行落库。CSV 文件不存在时退出码 1 且不留批次记录。`list-quarantine` 的来源名不存在时退出码 1、stderr 一行 `Error:`。
