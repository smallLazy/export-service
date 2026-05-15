# Export Service

独立 Go 数据导出服务，用于大数据 SQL 查询、异步任务管理、CSV 文件生成、OSS 上传、callback 通知业务系统，以及失败重试。

当前已实现 `member_order` 会员订单导出。后续新增发票、项目订单等导出类型时，只需要新增业务 exporter 并在 bootstrap 注册。

## 1. 项目目录说明

```text
.
├── main.go                         # 程序启动入口，负责 CLI/HTTP 启动
├── app.go                          # HTTP API 层，路由、请求解析、任务入口
├── filters.go                      # HTTP submit 上下文参数解析
├── cli_filters.go                  # CLI filter/query/params 参数解析
├── internal
│   ├── bootstrap
│   │   └── runtime.go              # DB、OSS、callback、registry、task、executor 组装
│   ├── callback
│   │   └── http.go                 # HTTP callback client
│   ├── config
│   │   └── config.go               # env 解析、配置校验、DSN 构造
│   ├── export
│   │   ├── registry.go             # Exporter 接口、导出类型注册/查找
│   │   └── executor.go             # 导出、上传、callback、retry 编排
│   ├── exporters
│   │   └── memberorder
│   │       ├── exporter.go          # 会员订单 SQL、查询、导出实现
│   │       ├── csv.go               # 会员订单 CSV header/row mapping
│   │       └── filters.go           # 会员订单筛选参数解析
│   ├── storage
│   │   └── aliyun_oss.go            # 阿里云 OSS 上传和签名 URL
│   ├── task
│   │   ├── task.go                  # Task、TaskManager、状态流转
│   │   └── mysql_store.go           # MySQL task 持久化
│   └── writer
│       └── csv.go                   # CSV 原子写入，.part + rename
└── .env.example                     # 环境变量示例
```

## 2. 启动方式

复制配置：

```bash
cp .env.example .env
```

启动 HTTP 服务：

```bash
go run . -env-file=.env -env=local
```

指定监听地址：

```bash
go run . -env-file=.env -env=local -addr=:8088
```

CLI 本地导出 CSV：

```bash
go run . \
  -mode=export \
  -type=member_order \
  -env-file=.env \
  -env=local \
  -filter time_options=payment_at \
  -filter 'time_keyword[]=2019-01-01' \
  -filter 'time_keyword[]=2026-04-30' \
  -output=./exports/member_orders.csv
```

CLI 支持三种筛选传参：

```bash
# repeatable key=value
-filter time_options=payment_at -filter 'time_keyword[]=2019-01-01'

# URL query
-query 'time_options=payment_at&time_keyword[]=2019-01-01&time_keyword[]=2026-04-30'

# JSON
-params '{"time_options":"payment_at","time_keyword":["2019-01-01","2026-04-30"]}'
```

## 3. 环境变量说明

基础配置：

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `APP_ENV` | `local` | 当前环境名。DB 配置支持环境前缀，例如 `PROD_DB_HOST` |
| `APP_ADDR` | `:8088` | HTTP 监听地址 |
| `EXPORT_OUTPUT_DIR` | `./exports` | 本地 CSV 输出目录 |
| `EXPORT_QUERY_TIMEOUT` | `5m` | 单个导出任务超时时间 |
| `EXPORT_MYSQL_BATCH_SIZE` | `1000` | MySQL 游标分页批次大小 |
| `EXPORT_SEED_SIZE` | `30000` | 无 DB 时内存 mock 数据量 |
| `EXPORT_USE_MEMORY_WHEN_NO_DB` | `true` | 无 DB 配置时是否启用内存数据 |
| `EXPORT_MAX_CONCURRENCY` | `3` | 最大并发导出任务数，`0` 表示不限制 |
| `EXPORT_MAX_RETRIES` | `2` | 导出 SQL/CSV 阶段最大重试次数 |
| `EXPORT_OSS_MAX_RETRIES` | `3` | OSS 上传最大重试次数 |

数据库配置：

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `DB_DSN` | 空 | 完整 MySQL DSN，设置后优先使用 |
| `DB_HOST` | `127.0.0.1` | MySQL host |
| `DB_PORT` | `3306` | MySQL port |
| `TRADE_DB_DATABASE` / `DB_DATABASE` | 空 | trade 数据库名，优先 `TRADE_DB_DATABASE` |
| `DB_USERNAME` | 空 | MySQL 用户名 |
| `DB_PASSWORD` | 空 | MySQL 密码 |
| `DB_CHARSET` | `utf8mb4` | MySQL charset |
| `DB_PARSE_TIME` | `true` | 是否解析时间 |
| `DB_LOC` | `Local` | MySQL 时区参数 |
| `DB_PREFIX` / `DB_TABLE_PREFIX` | `h_` | 业务表前缀 |
| `DB_MAX_OPEN_CONNS` | `10` | 最大连接数 |
| `DB_MAX_IDLE_CONNS` | `5` | 最大空闲连接数 |

不同环境可用前缀覆盖，例如 `APP_ENV=prod` 时优先读取：

```dotenv
PROD_DB_HOST=mysql.example.com
PROD_TRADE_DB_DATABASE=helix-trade
PROD_DB_USERNAME=export_user
PROD_DB_PASSWORD=secret
```

## 4. HTTP API 说明

健康检查：

```bash
curl http://127.0.0.1:8088/healthz
```

统一导出 API：

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `GET` | `/api/exports/{type}/count` | 统计筛选后的总数 |
| `GET` | `/api/exports/{type}/preview` | 预览前 N 条，默认 20，最大 200 |
| `GET` | `/api/exports/{type}/submit` | 创建异步导出任务 |
| `GET` | `/api/exports/{type}/export` | 兼容 submit，创建异步导出任务 |
| `GET` | `/api/exports/tasks/{task_id}` | 查询任务状态 |
| `POST` | `/api/exports/tasks/{task_id}/retry` | 重试任务 |
| `POST` | `/api/exports/tasks/{task_id}/cancel` | 取消任务 |
| `GET` | `/api/exports/tasks/{task_id}/download` | 下载本地 CSV |

兼容旧会员订单 API：

```text
GET /api/member-orders/count
GET /api/member-orders/preview
GET /api/member-orders/export
```

会员订单导出类型：

```text
member_order
member
member_orders
member-order
member-orders
```

count 示例：

```bash
curl -G 'http://127.0.0.1:8088/api/exports/member_order/count' \
  --data-urlencode 'time_options=payment_at' \
  --data-urlencode 'time_keyword[]=2019-01-01' \
  --data-urlencode 'time_keyword[]=2026-04-30'
```

submit 示例：

```bash
curl -G 'http://127.0.0.1:8088/api/exports/member_order/submit' \
  --data-urlencode 'user_id=10001' \
  --data-urlencode 'company_uuid=company-uuid' \
  --data-urlencode 'request_id=test_export_001' \
  --data-urlencode 'file_name=会员订单_test.csv' \
  --data-urlencode 'source_system=trade' \
  --data-urlencode 'callback_url=http://business-service/export/callback' \
  --data-urlencode 'time_options=payment_at' \
  --data-urlencode 'time_keyword[]=2019-01-01' \
  --data-urlencode 'time_keyword[]=2026-04-30'
```

无 callback 服务时，可以不传 `callback_url`：

```bash
curl -G 'http://127.0.0.1:8088/api/exports/member_order/submit' \
  --data-urlencode 'request_id=test_export_001' \
  --data-urlencode 'source_system=trade' \
  --data-urlencode 'time_options=payment_at' \
  --data-urlencode 'time_keyword[]=2019-01-01' \
  --data-urlencode 'time_keyword[]=2026-04-30'
```

## 5. submit 参数说明

业务上下文参数，Go 服务只保存并 callback 原样带回，不做权限校验：

| 参数 | 必填 | 说明 |
| --- | --- | --- |
| `user_id` | 否 | 当前业务用户 ID，用于业务系统回写用户导出列表 |
| `company_uuid` | 否 | 公司 UUID，通常用于业务侧展示或隔离 |
| `request_id` | 建议必填 | 业务请求幂等 ID。同一 `source_system + request_id + export_type` 会生成稳定 task id |
| `file_name` | 否 | 业务侧展示文件名 |
| `callback_url` | 否 | 业务服务 callback 地址。为空时跳过 callback |
| `source_system` | 建议必填 | 来源系统，例如 `trade` |

会员订单筛选参数：

| 参数 | 说明 |
| --- | --- |
| `time_options` | 时间字段：`payment_at` 或 `created_at` |
| `time_keyword[]` | 时间范围，传两次：开始日期、结束日期 |
| `payment_start` / `payment_end` | 旧版支付时间筛选兼容参数 |
| `options` | 搜索字段，例如 `mobile`、`nickname`、`user_number`、`order_no`、`member_title`、`payment_transaction_no` |
| `keyword` | 搜索关键词 |
| `status` | 订单状态 |
| `payment_status` | 支付状态 |
| `company_uuid` | 公司 UUID，同时也是业务上下文。作为筛选时会传入会员订单筛选 |
| `source` / `order_source` | 订单来源 |
| `refund_status` | 退款状态 |
| `payment_method` | 支付方式 |
| `payment_transaction_no` | 支付交易号模糊匹配 |
| `installment_type` | 分期类型 |
| `goods_type` | 商品类型 |
| `member_uuid` | 会员权益 UUID |
| `member_title` | 会员名称模糊匹配 |
| `amount` | 订单金额 |

注意：`user_id/request_id/file_name/callback_url/source_system` 会从导出筛选中剔除，只作为任务上下文。`company_uuid` 当前既会保存到任务上下文，也会保留为会员订单筛选条件。

## 6. callback payload 示例

callback 使用 `POST JSON`，HTTP 2xx 表示成功；非 2xx 或请求失败会记录 `callback_status=failed` 和 `callback_error`。

成功示例：

```json
{
  "task_id": "a981e31ba7dc01c3",
  "request_id": "test_export_001",
  "user_id": "10001",
  "company_uuid": "company-uuid",
  "source_system": "trade",
  "export_type": "member_order",
  "file_name": "会员订单_test.csv",
  "status": "completed",
  "file_size": 123456,
  "oss_key": "exports/member_order/2026/05/14/a981e31ba7dc01c3.csv",
  "oss_url": "https://example.com/exports/member_order/2026/05/14/a981e31ba7dc01c3.csv",
  "expired_at": "2026-05-21T10:00:00+08:00"
}
```

失败示例：

```json
{
  "task_id": "a981e31ba7dc01c3",
  "request_id": "test_export_001",
  "user_id": "10001",
  "company_uuid": "company-uuid",
  "source_system": "trade",
  "export_type": "member_order",
  "file_name": "会员订单_test.csv",
  "status": "failed",
  "error_message": "export failed"
}
```

业务服务收到 callback 后建议按 `request_id` 或 `task_id` 幂等更新用户导出列表/消息中心。

## 7. OSS 配置说明

OSS 配置：

| 变量 | 说明 |
| --- | --- |
| `OSS_ENDPOINT` | OSS endpoint，例如 `oss-cn-beijing.aliyuncs.com`，不要带 bucket |
| `OSS_BUCKET` | bucket 名，例如 `helixlife-image` |
| `OSS_ACCESS_KEY_ID` | AccessKey ID |
| `OSS_ACCESS_KEY_SECRET` | AccessKey Secret |
| `OSS_BASE_PATH` | 上传目录前缀，默认 `exports` |
| `OSS_CDN_DOMAIN` | 可选 CDN 域名。设置后返回 CDN URL |
| `OSS_ROLE_SESSION_NAME` | 预留字段，当前不执行 STS AssumeRole |
| `OSS_ROLE_ARN` | 预留字段，当前不执行 STS AssumeRole |
| `OSS_URL_EXPIRE_DAYS` | 签名 URL 过期天数，默认 7 |
| `EXPORT_OSS_MAX_RETRIES` | OSS 上传重试次数，默认 3 |

OSS key 格式：

```text
{OSS_BASE_PATH}/{export_type}/yyyy/mm/dd/{task_id}.csv
```

例如：

```text
exports/member_order/2026/05/14/a981e31ba7dc01c3.csv
```

上传规则：

- 本地 CSV 成功生成后才上传 OSS。
- 上传成功保存 `oss_key`、`oss_url`、`file_size`、`expired_at`、`oss_status=completed`。
- 上传失败设置 `status=failed`、`oss_status=failed`、`error_message=upload oss failed`、`oss_error=详细错误`。
- 上传失败 retry 时，如果本地 CSV 存在且 size > 0，只重试 OSS 上传和 callback，不重新跑 SQL。

## 8. task 状态说明

主状态 `state`：

| 状态 | 说明 |
| --- | --- |
| `pending` | 任务已创建，等待执行 |
| `running` | 任务执行中，包括导出或上传阶段 |
| `completed` | 导出完成，本地文件生成成功，OSS 上传成功或未配置 OSS |
| `failed` | 导出、上传或其他关键步骤失败 |
| `canceled` | 用户取消任务 |

OSS 状态 `oss_status`：

| 状态 | 说明 |
| --- | --- |
| 空 | 未配置 OSS 或尚未开始上传 |
| `uploading` | 正在上传 OSS |
| `completed` | OSS 上传成功 |
| `failed` | OSS 上传失败 |

callback 状态 `callback_status`：

| 状态 | 说明 |
| --- | --- |
| 空 | 未配置 callback 或尚未 callback |
| `pending` | callback 请求中 |
| `completed` | callback 成功 |
| `failed` | callback 失败 |

## 9. retry/cancel/download 规则

查询任务：

```bash
curl 'http://127.0.0.1:8088/api/exports/tasks/{task_id}'
```

重试任务：

```bash
curl -X POST 'http://127.0.0.1:8088/api/exports/tasks/{task_id}/retry'
```

强制重新导出：

```bash
curl -X POST 'http://127.0.0.1:8088/api/exports/tasks/{task_id}/retry?force_rebuild=true'
```

retry 规则：

- 导出阶段失败：重新跑 SQL，重新生成 CSV，再上传 OSS，再 callback。
- OSS 上传失败，且本地 CSV 存在并且 size > 0：只重试 OSS 上传，不重新跑 SQL。
- callback 失败：只重试 callback，不重新导出、不重新上传。
- 本地 CSV 不存在或为空：回退到重新导出。
- `force_rebuild=true`：强制重新跑 SQL 和重新生成 CSV。

取消任务：

```bash
curl -X POST 'http://127.0.0.1:8088/api/exports/tasks/{task_id}/cancel'
```

cancel 规则：

- `pending/running` 可以取消。
- `running` 会触发 context cancel。
- `completed` 不能取消。
- `canceled` 重复取消会返回当前任务状态。

下载本地文件：

```bash
curl -OJ 'http://127.0.0.1:8088/api/exports/tasks/{task_id}/download'
```

download 规则：

- 只支持 `completed` 任务。
- 当前保留本地 download 接口，即使存在 `oss_url` 也不强制跳转。
- 文件由本地 `.part` 临时文件成功 `Sync + Close + Rename` 后生成，失败会清理 `.part`。

## 10. 新增导出类型的开发步骤

以新增 `invoice` 发票导出为例：

1. 新建业务包：

```text
internal/exporters/invoice/
├── exporter.go
├── csv.go
└── filters.go
```

2. 实现 `internal/export.Exporter` 接口：

```go
type Exporter struct {
    repo Repository
}

func NewExporter(repo Repository) *Exporter {
    return &Exporter{repo: repo}
}

func (e *Exporter) Type() string {
    return "invoice"
}

func (e *Exporter) Aliases() []string {
    return []string{"invoice", "invoices"}
}

func (e *Exporter) Count(ctx context.Context, params url.Values) (int64, error) {
    // parse filters, call repo count
}

func (e *Exporter) Preview(ctx context.Context, params url.Values, limit int) ([]map[string]any, error) {
    // parse filters, return preview rows
}

func (e *Exporter) StreamCSV(ctx context.Context, target io.Writer, params url.Values) (int64, error) {
    // stream query result to CSV
}
```

3. 在 `internal/exporters/invoice/filters.go` 内解析发票自己的筛选参数。不要把发票筛选参数放进 `app.go`。

4. 在 `internal/exporters/invoice/csv.go` 内定义发票 CSV header 和 row mapping。字段顺序由业务 exporter 自己维护。

5. 在 [internal/bootstrap/runtime.go](/Users/eva/Projects/Me/Go/export-service/internal/bootstrap/runtime.go:96) 初始化发票 repository，并注册：

```go
r.Registry.Register(invoice.NewExporter(invoiceRepo))
```

6. 如需新业务表，新增独立 repository/table names。不要修改会员订单 SQL。

7. 验证：

```bash
go test ./...
curl -G 'http://127.0.0.1:8088/api/exports/invoice/count' ...
curl -G 'http://127.0.0.1:8088/api/exports/invoice/submit' ...
```

## 11. 上线前测试清单

基础启动：

- `go test ./...` 通过。
- `go run . -env-file=.env -env=local` 能启动。
- `/healthz` 返回 `ok=true`，并包含 `member_order` 类型。
- `.env` 必填项校验符合预期。

数据库：

- 生产/预发 DB 使用只读或低权限导出账号。
- `DB_MAX_OPEN_CONNS`、`EXPORT_MAX_CONCURRENCY` 与数据库承载能力匹配。
- `EXPORT_MYSQL_BATCH_SIZE` 在真实数据量下压测通过。
- `export_tasks` 表自动创建/升级成功。

会员订单导出：

- `count` 简单筛选正常。
- `count` 复杂筛选正常。
- `preview` 返回字段正常。
- `submit` 生成 task。
- 100w+ 数据量压测内存稳定。
- CSV header 和业务原导出一致。
- CSV 文件可被 Excel 正常打开。

任务流转：

- 并发满时返回 `429 Too Many Requests`。
- submit 幂等：同一 `request_id + source_system + type` 返回同一 task。
- cancel running 能取消，并最终为 `canceled`。
- completed 任务不能 cancel。
- retry export failed 会重新导出。
- retry OSS failed 本地文件存在时只重传 OSS。
- retry callback failed 只重调 callback。
- 服务重启后历史 task 可查询，`pending/running` 会标记为 failed。

OSS：

- `OSS_ENDPOINT` 不带 bucket。
- `OSS_BUCKET` 正确。
- `OSS_BASE_PATH` 不重复包含 bucket。
- 上传后 object key 符合 `{base_path}/{export_type}/yyyy/mm/dd/{task_id}.csv`。
- `oss_url` 可下载。
- 上传失败时不重新跑 SQL，retry 可恢复。

callback：

- `callback_url` 为空时跳过 callback。
- callback 使用 `POST JSON`。
- 业务服务返回 2xx 时 `callback_status=completed`。
- 业务服务返回非 2xx 或超时时 `callback_status=failed`。
- 业务服务按 `request_id` 或 `task_id` 幂等写入用户导出列表/消息中心。

前端/业务系统：

- 前端仍调用原业务服务，不直接依赖 Go 服务。
- 业务服务 submit 时传 `request_id/user_id/company_uuid/file_name/source_system/callback_url`。
- 业务服务收到 callback 后更新当前用户导出缓存/消息中心。
- 前端导出列表能展示处理中、成功、失败。
- 成功后优先使用业务系统返回的 OSS URL 下载。
