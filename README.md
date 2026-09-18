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

已有接口：`GET /` 显示数据目录与计数发布界面，`GET /health` 检查服务与数据库，`GET /api/datasets` 返回数据集名称、说明、合成标记及字段名称。接口不提供行级记录。

差分隐私接口（仅支持 `retail-demo` 合成数据）：

- `POST /api/releases`：请求体含 `request_id`、`dataset_id`、`filters`、`epsilon`。前两项为非空字符串且 `dataset_id` 必须为 `retail-demo`（未知数据集返回 404）；`filters` 可空，只允许 `region`、`membership`、`age_band`、`visit_bucket` 四个公开字段的字符串等值条件，按 AND 组合；`epsilon` 取 0.1–1.0。输入非法返回 422。
- 真实计数加入尺度 `1/epsilon` 的 Laplace 噪声，四舍五入（非银行家舍入）后下限为零。响应与 SQLite 审计仅含 `release_id`、原请求、`published_count`、`remaining_budget`、`created_at`，不保存 `member_id`、行级记录或真实计数。
- 总预算默认 3.0，可用 `PRIVSTAT_EPSILON_BUDGET` 配置。首次发布在同一 SQLite 事务中原子扣减 `epsilon`；余额不足返回 409 且不留记录。同一 `request_id`：规范化请求相同则返回原记录、不再扣减，不同则返回 409。该语义在重启与并发下均成立（写事务以 `BEGIN IMMEDIATE` 串行化）。
- `GET /api/privacy-budget` 返回 `initial`、`used`、`remaining`；`GET /api/releases` 按时间倒序列出历史。

启动时将 `data/retail_members.csv` 导入 SQLite，默认数据库为 `.runtime/privstat.sqlite3`；可用 `PRIVSTAT_DATABASE_PATH` 指定其他路径。重复启动保留已有记录（含发布历史与预算账本，故重启后幂等与预算状态仍然成立）。

```sh
.venv/bin/python -m privstat.demo --port 8000
.venv/bin/python -m unittest discover -s tests -v
```

演示命令在指定端口启动临时服务，展示实际页面与接口响应，并通过 `POST /api/releases` 发布一次加噪计数，随后输出隐私预算与发布历史，最后关闭自己启动的服务；端口已被占用时直接报错。演示数据不写入常用数据库。

当前仅提供合成数据上的差分隐私计数发布；接口与页面均不暴露 `member_id`、行级记录或真实计数，没有认证机制，不应接入真实个人数据。
