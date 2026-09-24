# 数据导入与校验台账

用于本地来源数据导入与校验记录管理的命令行项目。

需要 Python 3.12，无第三方依赖。在仓库根目录运行：

```bash
python3 -m import_pipeline --help
python3 -m import_pipeline --version
python3 -m unittest discover -s tests -v
```

无参数显示帮助，未知参数以非零状态退出。所有配置持久化到仓库根目录的
`import_ledger.db`（SQLite，标准库 `sqlite3` 创建），它是唯一的业务数据文件。

## 子命令

### 注册来源配置 `add-source`

```bash
python3 -m import_pipeline add-source \
  --name sales --path data/sales.csv \
  --field id --field amount --field region
```

- `--name`：来源名，非空字符串，唯一业务键；重复注册被拒绝（退出码 1），已保存的同名配置保持原样。
- `--path`：CSV 文件路径，非空。
- `--field`：必需字段名，可重复传入；至少一个，名称非空且不重复，按传入顺序登记。

成功时 stdout 输出一行：`Result: add-source <来源名> 1`。

### 注册字段映射 `add-mapping`

```bash
python3 -m import_pipeline add-mapping \
  --source sales --source-column 订单编号 --target-column order_id
```

- `--source`：必须是已注册的来源名，否则拒绝。
- `--source-column` / `--target-column`：源列名与目标列名，均为非空字符串。
- 同一来源下同一源列名重复登记视为冲突：拒绝本次操作，保留首次映射，不覆盖、不并存。

成功时 stdout 输出一行：`Result: add-mapping <来源名> 1`。

### 查询 `list-sources` / `list-mappings`

```bash
python3 -m import_pipeline list-sources
python3 -m import_pipeline list-mappings --source sales
```

- `list-sources`：每行一个来源，字段为 `来源名⇥文件路径⇥字段名列表`，字段名按注册顺序以逗号连接，以制表符分隔。
- `list-mappings`：每行一个映射，字段为 `源列名⇥目标列名`，按注册顺序以制表符分隔。
- 无记录时输出空内容、退出码 0；查询不存在的来源名退出码非 0 并写 stderr。

## 退出约定

- 成功：退出码 0。
- 参数缺失或未知子命令/未知参数：argparse 用法错误，退出码 2（未知参数的字面值出现在 stderr）。
- 业务校验失败（重名、空值、重复字段、来源不存在等）：退出码 1，错误信息写 stderr，不输出 `Result:` 行，数据库保持操作前状态（单次操作原子提交，无部分写入）。
