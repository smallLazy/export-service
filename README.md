# Export Service

Go 导出服务，`main.go` 只负责启动、CLI 参数、HTTP 路由和导出类型分发；具体导出查询放在各自功能文件里。

当前已实现：

- 统一 HTTP 入口：`/api/exports/{type}/{count|preview|export}`
- 统一 CLI 入口：`go run . -mode=export -type=member_order ...`
- 会员订单导出：`member_order`，兼容别名 `member`、`member-orders`
- 配置从 `.env` 读取，支持不同环境数据库
- 会员订单按 AdminOperate 的筛选方式对齐：`options + keyword`、`time_options + time_keyword`
- MySQL 分批游标导出，边查边写 CSV
- CLI 导出每 1000 行输出一次进度日志
- 会员订单常见筛选会先分页订单 ID，再只聚合本批订单详情，减少全量聚合开销

## 目录职责

- `main.go`：程序入口、CLI 参数、启动 HTTP/CLI 导出
- `app.go`：统一导出入口、exporter 注册、HTTP 路由、CSV 通用写入
- `config.go`：读取 `.env` 和环境变量，组装数据库配置
- `runtime.go`：初始化数据库连接、表名前缀、业务 repository
- `filters.go`：通用时间/搜索辅助方法
- `cli_filters.go`：CLI 的 `-filter`、`-query`、`-params` 参数解析
- `member_order.go`：会员订单导出器和 repository
- `member_order_filters.go`：会员订单筛选参数解析
- `member_order_csv.go`：会员订单 CSV 表头和行映射

后面新增分期订单、发票导出时，建议新增：

- `installment_order.go`
- `invoice.go`

每个导出类型实现 `Exporter`，再在 `newApp` 里注册即可。入口层只传原始参数，具体字段由各 exporter 自己解析。

## Env 配置

先复制样例：

```bash
cp .env.example .env
```

常用配置：

```dotenv
APP_ENV=local
APP_ADDR=:8088

EXPORT_OUTPUT_DIR=./exports
EXPORT_QUERY_TIMEOUT=5m
EXPORT_MYSQL_BATCH_SIZE=1000

DB_HOST=127.0.0.1
DB_PORT=3306
TRADE_DB_DATABASE=helix-trade
DB_USERNAME=root
DB_PASSWORD=
DB_PREFIX=h_
```

如果不同环境数据库不同，有两种方式。

方式一：用不同 env 文件：

```bash
go run . -env-file=.env.local -env=local
go run . -env-file=.env.prod -env=prod
```

方式二：在同一个 `.env` 里加环境前缀。比如 `APP_ENV=prod` 时，`PROD_` 开头的配置会优先：

```dotenv
APP_ENV=prod

DB_HOST=127.0.0.1
TRADE_DB_DATABASE=helix-trade-local

PROD_DB_HOST=mysql.example.com
PROD_TRADE_DB_DATABASE=helix-trade
PROD_DB_USERNAME=export_user
PROD_DB_PASSWORD=secret
PROD_DB_PREFIX=h_
```

也支持直接写 DSN：

```dotenv
PROD_DB_DSN=export_user:secret@tcp(mysql.example.com:3306)/helix-trade?charset=utf8mb4&parseTime=true&loc=Local
```

## 启动 HTTP

```bash
go run . -env-file=.env -env=local
```

接口：

- `GET /healthz`
- `GET /api/exports/member_order/count`
- `GET /api/exports/member_order/preview`
- `GET /api/exports/member_order/export`

示例：

```text
http://127.0.0.1:8088/api/exports/member_order/export?time_options=payment_at&time_keyword[]=2025-05-01&time_keyword[]=2026-04-30
```

## CLI 导出

CLI 只保留入口级参数，业务筛选统一用 `-filter`、`-query` 或 `-params` 传给对应 exporter。

推荐写法：

```bash
go run . \
  -mode=export \
  -type=member_order \
  -env-file=.env \
  -env=local \
  -filter time_options=payment_at \
  -filter 'time_keyword[]=2025-05-01' \
  -filter 'time_keyword[]=2026-04-30' \
  -output=./exports/member_orders.csv
```

如果不传 `-output`，会导出到 `EXPORT_OUTPUT_DIR`，文件名自动生成。

也可以直接复用 HTTP query：

```bash
go run . \
  -mode=export \
  -type=member_order \
  -query 'time_options=payment_at&time_keyword[]=2025-05-01&time_keyword[]=2026-04-30'
```

或者传 JSON：

```bash
go run . \
  -mode=export \
  -type=member_order \
  -params '{"time_options":"payment_at","time_keyword":["2025-05-01","2026-04-30"],"options":"mobile","keyword":"13800138000"}'
```

## 会员订单筛选

支持直接筛选：

- `status`
- `payment_status`
- `company_uuid`
- `source` / `order_source`
- `refund_status`
- `payment_method`
- `payment_transaction_no`
- `installment_type`
- `goods_type`
- `member_uuid`
- `member_title`
- `amount`

支持 `options + keyword`：

- `mobile`
- `nickname`
- `user_number` / `user_id`
- `no` / `order_no`
- `title` / `member_title`
- `payment_transaction_no`

支持时间筛选：

- `time_options=payment_at`
- `time_options=created_at`
- `time_keyword[]=2025-05-01&time_keyword[]=2026-04-30`

CLI 对应：

```bash
-filter time_options=payment_at -filter 'time_keyword[]=2025-05-01' -filter 'time_keyword[]=2026-04-30'
```
