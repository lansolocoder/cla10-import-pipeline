# 数据导入与校验台账

用于本地来源数据导入与校验记录管理的命令行项目。

需要 Python 3.12，无第三方依赖。在仓库根目录运行：

```bash
python3 -m import_pipeline --help
python3 -m import_pipeline --version
python3 -m unittest discover -s tests -v
```

当前仅提供帮助与版本查询入口；无参数显示帮助，未知参数以非零状态退出。尚未实现来源映射、增量导入、校验失败隔离以及重跑与结果台账，不会创建业务数据文件。
