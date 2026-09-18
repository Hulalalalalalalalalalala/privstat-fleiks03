# PrivStat

本地隐私统计应用的数据目录外壳，使用固定的合成零售会员数据。

需要 Python 3.11 或以上版本。在仓库根目录执行：

```sh
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
.venv/bin/python -m privstat --port 8000
```

访问 `http://127.0.0.1:8000`。Windows 可将 `.venv/bin/python` 换成 `.venv\Scripts\python.exe`。

系统缺少 `ensurepip` 时，可用已安装的 `uv venv .venv` 和 `uv pip install --python .venv/bin/python -r requirements.txt` 创建环境并安装依赖。

已有接口：`GET /` 显示数据目录，`GET /health` 检查服务与数据库，`GET /api/datasets` 返回数据集名称、说明、合成标记及字段名称。接口不提供行级记录。

启动时将 `data/retail_members.csv` 导入 SQLite，默认数据库为 `.runtime/privstat.sqlite3`；可用 `PRIVSTAT_DATABASE_PATH` 指定其他路径。重复启动保留已有记录。

```sh
.venv/bin/python -m privstat.demo --port 8000
.venv/bin/python -m unittest discover -s tests -v
```

演示命令在指定端口启动临时服务，展示实际页面与接口响应后关闭自己启动的服务；端口已被占用时直接报错。演示数据不写入常用数据库。

当前仅提供合成数据目录，没有认证、统计发布或隐私保护算法，不应接入真实个人数据。
