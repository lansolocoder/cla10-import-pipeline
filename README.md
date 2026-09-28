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

# 业务操作：按给定顺序把同一来源的多个 CSV 合并为单个批次导入
python3 -m import_pipeline batch-import orders data/part1.csv data/part2.csv

# 只读查询：输出来源自给定批次号起（含）的各批次，
# 每行：批次号、状态、成功行数、被隔离行数（制表符分隔，按批次号升序）
python3 -m import_pipeline show-batch orders 1

# 业务撤销：撤回某来源指定批次号的单个批次
python3 -m import_pipeline revoke-batch orders 1
```

`run-import` 的 CSV 首行为表头，数据行按已登记的字段映射写入 `imported_rows`。出现未映射的源列、必需字段缺少映射目标对应列，或数据行的值为空白时整批拒绝：退出码 1、stderr 一行 `Error:`，批次状态记为 `rejected`，已校验行不落库；否则整批落库，退出码 0，stdout 输出 `Result: run-import <来源名> <成功行数>`，批次状态记为 `ok`。CSV 文件不存在时退出码 1 且不留批次记录。无论成功或拒绝都会在 `batches` 表留下批次记录：批次号（来源内从 1 递增）、来源名、状态、成功行数、被隔离行数。重复执行视为新批次，已有批次记录不变。`show-batch` 的来源名或批次号不存在时退出码 1、stderr 一行 `Error:`。

`batch-import <来源名> <CSV路径>...` 把同一来源的多次导出合并为单个批次：至少两个文件，按命令行给定的文件顺序读取，每个文件首行为表头，数据行按已登记字段映射落库；行号按文件参数顺序、文件内行序从 1 起连续分配（跨文件连续），批次号仍为来源内从 1 递增。全部通过时整批在同一事务内原子落库，批次状态 `ok`，退出码 0，stdout 单行 `Result: batch-import <来源名> <成功行数>`（成功行数为全部文件数据行总数）。批内重复目标列值照常落库不合并，重复检测仍由 `find-dupes` 负责（同批与跨批同值行合并统计）。成功后 `show-rows` 该批次按行号升序输出全部行的规范化 JSON，跨文件行连续。校验规则沿用 `run-import`：每个文件表头的源列都在已登记映射中、每个必需字段都有映射目标对应列、数据行列数与表头一致且值非空白，任一不满足即整批拒绝，批次状态 `rejected`、被隔离行数为全部文件数据行总数、已校验行不落库，退出码 1、stderr 一行 `Error:`、stdout 无输出。文件少于两个、任一文件不存在或无法读取、缺表头、表头出现空白列名、或两个文件表头列名集合不一致时，退出码 1、stderr 一行 `Error:`、stdout 无输出，且不产生批次记录、数据库保持执行前状态；文件列表里的重复路径按普通文件对待。来源名校验沿用现有规则（大小写不同的保留字面值如 `TRUE` 非法）。`batch-import` 是业务写操作，单批次多文件读写原子完成，中途失败整体回滚，不留部分行或部分批次记录。

`revoke-batch <来源名> <批次号>` 用于业务回滚：仅允许撤销状态为 `ok` 的批次。撤销成功时退出码 0，stdout 输出 `Result: revoke-batch <来源名> <批次号>`，该批次状态变为 `revoked`、其 `imported_rows` 行全部删除，成功行数与被隔离行数保留撤销前数值不变；其他批次的批次记录与已导入行一律保持原状。删除行与状态翻转在同一事务内原子完成，中途任何失败都会整体回滚，不留部分删除。对同一批次再次撤销，或对状态非 `ok`（如 `rejected`、`revoked`）的批次撤销，退出码 1、stderr 一行 `Error:`、stdout 无输出且数据库不变；来源名或批次号不存在同样拒绝。校验沿用既有规则：来源名须为非空字符串且大小写不同的保留字面值（如 `TRUE`）视为非法；批次号为正整数，零或负数按业务规则拒绝（退出码 1、stderr 一行 `Error:`）。撤销后可用 `show-batch` 查看，该批次状态列显示小写 `revoked`。

## 行级只读查询

```bash
# 只读查询：列出某来源指定批次的已导入行，
# 每行：行号、该行数据的规范化 JSON（键按目标列名字典序排序，值为字符串），
# 制表符分隔，按行号升序
python3 -m import_pipeline show-rows orders 1

# 只读查询：按目标列值统计未撤销批次已落库行的重复组，
# 每行：目标列值、出现次数、逗号分隔的升序去重批次号列表，
# 制表符分隔，按目标列值升序
python3 -m import_pipeline find-dupes orders amount

# 只读查询：汇总来源在批次号闭区间内各状态的批次数与行数合计，
# 输出一行：批次总数、ok 批次数、rejected 批次数、revoked 批次数、
# 成功行合计、被隔离行合计（制表符分隔）
python3 -m import_pipeline batch-stats orders 1 3

# 只读查询：输出区间内指定状态批次的明细，
# 每行：批次号、成功行数、被隔离行数（制表符分隔，按批次号升序）
python3 -m import_pipeline batch-stats-detail orders 1 3 ok

# 只读查询：一次扫描同时给出区间内 ok、rejected、revoked 三段的汇总与明细
python3 -m import_pipeline batch-report orders 1 3

# 只读查询：一次只读扫描内对账 batch-report 与 batch-stats/batch-stats-detail 两路口径
python3 -m import_pipeline batch-reconcile orders 1 3
```

`show-rows` 仅 `ok` 批次有行；`rejected` 批次本无行、`revoked` 批次的行已在撤销时删除，两者均输出为空且退出码 0。来源名或批次号不存在时退出码 1、stderr 一行 `Error:`、stdout 无输出；批次号校验沿用既有规则（正整数，零或负数拒绝）。

`find-dupes` 统计当前未撤销批次（`ok` 与 `rejected`）中已落库的 `imported_rows`：同一目标列值出现于两行及以上即为一个重复组；已撤销批次的行不参与统计，因此撤销批次后重复组相应缩小。目标列不存在于任何已落库行、来源不存在、来源名非法或目标列名为空时，退出码 1、stderr 一行 `Error:`、stdout 无输出。

`batch-stats <来源名> <起始批次号> <结束批次号>` 在一次只读扫描里汇总批次号闭区间（含两端）内的批次：输出一行六个制表符分隔的十进制数字，依次为批次总数、`ok` 批次数、`rejected` 批次数、`revoked` 批次数、成功行合计、被隔离行合计。成功行合计与被隔离行合计为区间内各批次记录值之和；`revoked` 批次按撤销时保留的撤销前数值计入两个行数合计，但不计入 `ok` 批次数。区间内没有任何批次时输出一行全零（六列均为 `0`）、退出码 0。来源不存在、来源名非法（非空校验沿用现有规则，大小写不同的保留字面值如 `TRUE` 视为非法）、任一一个批次号不是正整数（零、负数拒绝）、或起始批次号大于结束批次号时，退出码 1、stderr 一行 `Error:`、stdout 无输出。查询本身不改变任何批次状态，也不影响后续 `show-batch`、`show-rows`、`find-dupes` 的结果。

`batch-stats-detail <来源名> <起始批次号> <结束批次号> <状态>` 在一次只读扫描里输出批次号闭区间（含两端）内指定状态批次的明细：每行三个制表符分隔的十进制数字，依次为批次号、成功行数、被隔离行数，按批次号升序。状态只接受小写 `ok`、`rejected`、`revoked`；`revoked` 批次沿用撤销时保留的撤销前数值原样输出两个行数，`rejected` 批次输出其记录的被隔离行数、成功行数为 `0`。每行的状态与两个行数来自同一次只读扫描的一致快照，因此同一区间同一状态的明细行数等于 `batch-stats` 中该状态批次数、明细两个行数合计等于 `batch-stats` 对应状态的行数合计。区间内没有任何该状态的批次（包括区间内有批次但都不是所查状态）时输出为空、退出码 0。来源不存在、来源名非法、任一批次号不是正整数、起始批次号大于结束批次号或状态取值非法（如 `OK`、未知状态）时，退出码 1、stderr 一行 `Error:`、stdout 无输出。查询不改变任何批次状态，也不影响 `show-batch`、`show-rows`、`find-dupes`、`batch-stats` 的结果。

`batch-report <来源名> <起始批次号> <结束批次号>` 在一次只读扫描里给出批次号闭区间（含两端）内 `ok`、`rejected`、`revoked` 三类状态的汇总与明细，三段数字同出一份一致快照。输出按 `ok`、`rejected`、`revoked` 固定顺序分三段，段间单独一行 `---` 分隔（某段无批次也保留该段）。每段首行为汇总行 `<状态>:<批次数>:<成功行合计>:<被隔离行合计>`（后三列为十进制数字、冒号分隔），随后为该状态的明细行，每行三个制表符分隔的十进制数字：批次号、成功行数、被隔离行数，按批次号升序。行数语义与 `batch-stats`、`batch-stats-detail` 一致：`revoked` 批次按撤销前保留值输出并计入合计，`rejected` 批次成功行数为 `0`、被隔离行数为记录值；该状态无批次时汇总行计数全 `0`、无明细行。三段的批次数与行数合计与同区间 `batch-stats` 一致，各段明细与同区间同状态的 `batch-stats-detail` 一致。区间内没有任何批次时照常输出三段结构（三段汇总行均全零）、退出码 0。来源不存在、来源名非法（大小写不同的保留字面值如 `TRUE` 视为非法）、任一批次号不是正整数（零、负数拒绝）、或起始批次号大于结束批次号时，退出码 1、stderr 一行 `Error:`、stdout 无输出且数据库不变。查询不改变任何批次状态，也不影响 `show-batch`、`show-rows`、`find-dupes`、`batch-stats`、`batch-stats-detail` 的结果。

`batch-reconcile <来源名> <起始批次号> <结束批次号>` 在一次只读扫描内对账两路口径：第一路为 `batch-report` 的三段汇总与明细，第二路为 `batch-stats` 的六列与 `batch-stats-detail` 的三状态明细。两路数字同出一份快照，批次号闭区间（含两端），`revoked` 批次按撤销前保留值比较，`rejected` 批次成功行数按 `0` 比较，明细按批次号升序。九项对账字段依次为 `total_batches`、`ok_batches`、`rejected_batches`、`revoked_batches`、`succeeded_rows`、`quarantined_rows`、`ok_detail`、`rejected_detail`、`revoked_detail`；汇总字段比较两路数字，明细字段比较两路明细的规范化文本——每批为批次号、成功行数、被隔离行数逗号连接，批间分号连接，按批次号升序，无明细为空串。全部一致时 stdout 输出单行 `Result: batch-reconcile <来源名> <起始批次号> <结束批次号> consistent`、退出码 0；任一对应口径不一致时，每项差异输出一行 `Mismatch: <字段> batch-report=<值> other=<值>`，差异按上述字段顺序排列，不打印一致结论，退出码 1。来源不存在、来源名非法（大小写不同的保留字面值如 `TRUE` 视为非法）、任一批次号不是正整数（零、负数拒绝）、或起始批次号大于结束批次号时，退出码 1、stderr 一行 `Error:`、stdout 无输出且数据库不变。查询为纯只读（拒绝路径同样不写入或修改 `import_ledger.db`），不改变任何批次状态，也不影响 `show-batch`、`show-rows`、`find-dupes`、`batch-stats`、`batch-stats-detail`、`batch-report` 的结果。

六个查询均为只读：任何情况下（包括拒绝路径）都不写入或修改 `import_ledger.db`。
