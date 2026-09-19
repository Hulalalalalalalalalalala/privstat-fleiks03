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

`POST /api/releases` 发布差分隐私计数：请求体含 `request_id`、`dataset_id`（仅支持 `retail-demo`，其他返回 404）、`filters`（可空，仅允许 `region`、`membership`、`age_band`、`visit_bucket` 四个公开字段的字符串等值条件，按 AND 组合）与 `epsilon`（0.1–1.0），非法输入返回 422。计数加入尺度 1/ε 的 Laplace 噪声，四舍五入并下限为零后发布；真实计数不写入任何响应或审计记录。同一 `request_id` 的规范化请求相同则返回原记录且不再扣减预算，不同则返回 409。

隐私预算总额默认为 3.0，可用 `PRIVSTAT_EPSILON_BUDGET` 调整。每次首发原子扣减 ε，余额不足返回 409 且不留记录；扣减与幂等记录在 SQLite 中持久化，重启与并发下均成立。`GET /api/privacy-budget` 返回初始、已用与剩余额度，`GET /api/releases` 按时间倒序返回发布历史。首页支持筛选发布、预算与历史展示及可读错误提示。

`GET /api/releases/export` 面向合作方导出已成功发布的历史，只读访问，不扣减预算、不创建审计记录，也不读取 `retail_members`。查询参数：`format` 为 `json`（默认，返回对象数组）或 `csv`（UTF-8 文本，首行固定为 `release_id,request_id,dataset_id,filters,epsilon,published_count,remaining_budget,created_at`，`filters` 用规范化 JSON 字符串编码）；`dataset_id` 可选且只能为 `retail-demo`；`request_id` 可选精确匹配（无匹配返回 200 空结果）；`from`/`to` 可选 ISO-8601 时间（无时区按 UTC，`from` 包含、`to` 不包含，按 `created_at` 的 UTC 时刻比较）；`limit` 默认 50，范围 1–100。结果按 `created_at` 倒序并截取前 `limit` 条。每条记录仅含与发布历史相同的公开字段，不含 `member_id`、真实计数或行级数据。`format`、`dataset_id`、时间或 `limit` 非法，或 `from` 不早于 `to` 时返回 422。首页提供导出入口，可预览 JSON 或下载 CSV，并展示可读错误提示。

启动时将 `data/retail_members.csv` 导入 SQLite，默认数据库为 `.runtime/privstat.sqlite3`；可用 `PRIVSTAT_DATABASE_PATH` 指定其他路径。重复启动保留已有记录。

```sh
.venv/bin/python -m privstat.demo --port 8000
.venv/bin/python -m unittest discover -s tests -v
```

演示命令在指定端口启动临时服务，展示实际页面与接口响应，发布一次计数并输出预算与历史后关闭自己启动的服务；端口已被占用时直接报错。演示数据不写入常用数据库。

当前仅提供合成数据目录与差分隐私计数发布，没有认证机制，不应接入真实个人数据。
