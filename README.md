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

`GET /api/releases/export` 面向合作方导出已成功发布的历史，只读、不扣减预算也不产生审计记录。查询参数：`format` 为 `json`（默认）或 `csv`；`dataset_id` 可省略，提供时只能为 `retail-demo`；`request_id` 可省略，精确匹配，显式空值 `request_id=` 返回 200 空结果（纯空白仍返回 422）；`from`/`to` 为可选 ISO-8601 时间，`from` 包含、`to` 不包含，按 `created_at` 的实际时刻精确比较，不截断到毫秒；`limit` 默认 50，范围 1–100。结果按 `created_at` 倒序并截取前 `limit` 条。`format`、`dataset_id`、时间或 `limit` 非法，或 `from` 不早于 `to`，返回 422；`request_id` 无匹配返回 200 空结果。JSON 返回对象数组；CSV 返回 UTF-8 文本，首行固定为 `release_id,request_id,dataset_id,filters,epsilon,published_count,remaining_budget,created_at`，其中 `filters` 为规范化 JSON 字符串。导出仅读取 releases 表的公开字段，不读取 `retail_members`，也不返回 `member_id`、真实计数或任何行级数据。首页提供导出表单并展示可读错误提示。

`POST /api/shares` 创建可撤销、可过期的合作方分享：请求体含 `dataset_id`（仅支持 `retail-demo`）、可选 `request_id`、`from`、`to`、`limit`（默认 50，范围 1–100）与必填 `expires_at`；时间须为带时区的 ISO-8601，窗口前含后不含，`expires_at` 须在未来，非法输入或 `from` 不早于 `to` 返回 422。范围持久化到 SQLite，返回 201 及不可猜测的 `share_id`、`token`、规范化范围与创建/过期时间。**原始 `token` 仅在此 201 响应中出现一次**，数据库只保存其 SHA-256 摘要（`token_digest`）；token 与摘要都不会出现在发布历史、导出或分享列表中。`GET /api/shares/{token}/releases` 仅返回范围内已成功发布的记录，字段与发布记录一致，按 `created_at` 倒序并受 `limit` 限制；未知 token 返回 404，过期或已撤销返回 410；访问为只读，不读取 `retail_members`，不扣减预算也不新增发布记录。

`GET /api/shares` 为管理端列表：可按 `status=active|expired|revoked`、`dataset_id`、`request_id` 精确筛选，`limit` 默认 50、范围 1–100；非法状态、未知数据集、空白 `request_id`（含显式 `request_id=`）或非法 `limit` 返回 422。结果按 `created_at` 倒序，返回创建字段（`share_id`、`dataset_id`、`request_id`、`from`、`to`、`limit`、`created_at`、`expires_at`）及 `revoked_at`、`status`；状态按当前 UTC 在数据库内计算，撤销优先于过期（既撤销又过期记为 `revoked`），任何情况下都不返回 token 或摘要。该接口只读 `shares` 表，不读取 `retail_members`、不扣减预算、不新增发布记录。

`DELETE /api/shares/id/{share_id}` 按 `share_id` 撤销：对已存在的分享原子、幂等返回 204（重复撤销保留首次 `revoked_at`），未知 `share_id` 返回 404；提交后通过 token 访问返回 410，并发撤销只形成一个持久状态。`DELETE /api/shares/{token}` 按原始 token 撤销，同样原子幂等，**对未知 token 也返回 204**（无法借此探测 token 是否存在）。

`POST /api/shares/id/{share_id}/rotate` 轮换分享凭证：请求体仅含非空字符串 `rotation_id`。原始 token 为第 1 代，每次成功轮换在最高版本上加一（首次返回 `token_version=2`），在单事务内生成新 token 并使旧代失效，返回 201 及 `share_id`、`rotation_id`、`token`、`token_version`、UTC 微秒级 `rotated_at`；范围、期限与 `share_id` 不变。**新 token 仅在此 201 响应中出现一次**，SQLite 的 `share_tokens` 表只保存各代 token 的 SHA-256 摘要。此后仅新 token 可访问，旧 token 访问返回 410；旧 token 撤销仍返回 204 且不影响当前分享，当前 token 与 `share_id` 撤销沿用原语义。未知 `share_id` 返回 404；分享已撤销或按当前 UTC 已过期时，未成功过的 `rotation_id` 返回 409 且状态不变。同一分享的 `rotation_id` 幂等：成功后的重试优先于过期、撤销判定，固定返回 200 及原元数据（不含 token），不改变状态；并发同标识只生成一代，任何时刻至多一代有效，代际、幂等与失效状态重启后保持。轮换不读取 `retail_members`、不扣减预算、不新增发布记录，token 与摘要不会进入列表、历史或导出。

启动时把既有发布记录的 `created_at` 回填为可索引的 UTC 微秒 ISO 字符串并建立索引；导出与分享范围的时间窗口（前含后不含）、倒序与限量均在数据库内完成，精确到实际时刻、不截断到毫秒。启动时还会幂等迁移旧库 `shares` 表中的明文 token：整表重命名后逐行写入 SHA-256 摘要，整个迁移在单事务内完成，失败即回滚（不丢失任何分享），成功后 `VACUUM` 清除文件中的明文残留；旧链接凭原 token 仍可访问与撤销，任何接口都不会泄露摘要。

首页在既有创建/复制/撤销能力之外，新增分享管理区：从服务端加载分享列表、按状态筛选、按 `share_id` 撤销；管理操作只读 `shares` 表，不读取 `retail_members`、不扣减预算、不新增发布记录。创建区与管理区均提供凭证轮换：自动生成 `rotation_id` 提交轮换，成功后在页面上一次性展示新令牌并提供复制按钮，轮换失败（如分享已撤销或过期）显示可读错误。

启动时将 `data/retail_members.csv` 导入 SQLite，默认数据库为 `.runtime/privstat.sqlite3`；可用 `PRIVSTAT_DATABASE_PATH` 指定其他路径。重复启动保留已有记录。

```sh
.venv/bin/python -m privstat.demo --port 8000
.venv/bin/python -m unittest discover -s tests -v
```

演示命令在指定端口启动临时服务，展示实际页面与接口响应，发布一次计数并输出预算与历史后关闭自己启动的服务；端口已被占用时直接报错。演示数据不写入常用数据库。

当前仅提供合成数据目录与差分隐私计数发布，没有认证机制，不应接入真实个人数据。
