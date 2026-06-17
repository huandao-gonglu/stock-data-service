# Stock Data Service

Stock Data Service 是一个独立的 A 股行情数据服务。它可以从本地缓存或百度网盘同步 zip 行情文件，将数据写入按股票和月份分区的 Parquet 文件，用 DuckDB 维护同步元数据，并通过 FastAPI 提供 K 线查询、覆盖率检查和管理页面。

## 功能概览

- 支持 `1m`、`5m`、`15m`、`30m`、`60m`、`1d` 周期。
- 支持从本地 zip 缓存导入，也支持从百度网盘下载并导入。
- Parquet 使用写时合并，按 `timeframe/symbol/year/month` 分区保存。
- DuckDB 记录远端文件、文件导入结果和每日覆盖率。
- FastAPI 提供查询接口、覆盖率接口、百度授权状态接口和管理 API。
- 内置管理页面可配置默认同步参数、浏览百度网盘目录、启动或停止同步任务。
- 日志会按天写入文件，并自动脱敏 token、API key 和 bearer 凭证。

## 环境要求

- Python 3.11 或更高版本
- 可访问的百度网盘开放平台应用，用于百度网盘同步
- 推荐使用虚拟环境运行

## 快速开始

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -e ".[dev]"
pytest
uvicorn stock_data_service.main:app --reload
```

启动后可以访问：

```text
http://127.0.0.1:8000/health
http://127.0.0.1:8000/admin
```

## 配置说明

复制示例配置后再填写本机真实值：

```bash
cp .env.example .env
```

`.env` 只用于本地运行，不要提交到仓库。当前支持的配置项如下：

```text
STOCK_DATA_DATA_ROOT=./data
STOCK_DATA_META_DB=./data/meta/sync_metadata.duckdb
STOCK_DATA_LOG_DIR=./data/logs
STOCK_DATA_LOG_LEVEL=INFO
STOCK_DATA_SERVER_MODE=0
DATA_API_KEY=
ADMIN_API_KEY=
BAIDU_APP_KEY=
BAIDU_APP_SECRET=
BAIDU_TOKEN_FILE=./baidu_token.json
BAIDU_CACHE_DIR=./data/raw/baidu
BAIDU_REDIRECT_URI=
BAIDU_SCOPE=basic,netdisk
```

配置含义：

| 配置项 | 说明 |
| --- | --- |
| `STOCK_DATA_DATA_ROOT` | 数据根目录，默认是 `./data`。 |
| `STOCK_DATA_META_DB` | DuckDB 元数据库路径，默认在 `data/meta/sync_metadata.duckdb`。 |
| `STOCK_DATA_LOG_DIR` | 日志目录，默认在 `data/logs`。 |
| `STOCK_DATA_LOG_LEVEL` | 日志级别，默认是 `INFO`。 |
| `STOCK_DATA_SERVER_MODE` | 服务模式开关，设为 `1` 后启用 API 鉴权。 |
| `DATA_API_KEY` | 查询接口使用的 API key。 |
| `ADMIN_API_KEY` | 管理页面和管理 API 使用的 API key。 |
| `BAIDU_APP_KEY` | 百度网盘开放平台应用 key。 |
| `BAIDU_APP_SECRET` | 百度网盘开放平台应用 secret。 |
| `BAIDU_TOKEN_FILE` | 百度授权 token 文件路径。 |
| `BAIDU_CACHE_DIR` | 百度网盘原始 zip 文件缓存目录。 |
| `BAIDU_REDIRECT_URI` | 百度 OAuth 回调地址。为空时使用当前服务的 `/admin/api/baidu/oauth/callback`。生产部署建议显式配置为百度开放平台登记的完整 URL。 |
| `BAIDU_SCOPE` | 百度 OAuth 授权范围，默认 `basic,netdisk`。 |

## 命令行使用

从已有本地缓存导入：

```bash
stock-data ingest-local \
  --raw-root ../StockK/download_cache \
  --data-root ./data \
  --timeframe 1m \
  --start 2024-12-20 \
  --end 2024-12-31 \
  --symbol sh600000
```

从百度网盘下载并导入：

```bash
BAIDU_APP_KEY=你的应用Key \
BAIDU_APP_SECRET=你的应用Secret \
BAIDU_TOKEN_FILE=./baidu_token.json \
stock-data sync-baidu \
  --data-root ./data \
  --timeframe 1m \
  --start 2024-12-20 \
  --end 2024-12-31 \
  --symbol sh600000
```

只下载百度网盘原始文件，不导入 Parquet：

```bash
stock-data download-baidu \
  --data-root ./data \
  --timeframe 1m \
  --start 2024-12-20 \
  --end 2024-12-31
```

`sync-baidu` 会根据路径策略寻找百度网盘候选文件，将每日 zip 下载到 `data/raw/baidu`，记录远端文件信息，然后把指定股票写入 Parquet 分区，并更新文件导入记录和每日覆盖率。

## HTTP 接口

健康检查：

```bash
curl "http://127.0.0.1:8000/health"
```

查询 K 线：

```bash
curl "http://127.0.0.1:8000/bars?symbol=sh600000&timeframe=1m&start=2024-12-20T09:30:00%2B08:00&end=2024-12-20T15:01:00%2B08:00"
```

覆盖率摘要：

```bash
curl "http://127.0.0.1:8000/coverage/summary?symbol=sh600000&timeframe=1m"
```

覆盖率缺口：

```bash
curl "http://127.0.0.1:8000/coverage/gaps?symbol=sh600000&timeframe=1m&start=2024-12-20&end=2024-12-31"
```

时间语义：

- 交易时间按 `Asia/Shanghai` 处理。
- Parquet 中保存无时区时间戳，但语义固定为上海时区。
- HTTP 响应会带 `+08:00`。
- 查询区间是左闭右开：`[start,end)`。

## 管理页面

浏览器打开：

```text
http://127.0.0.1:8000/admin
```

管理页面支持：

- 保存默认同步参数。
- 查看百度授权状态，并在未授权时弹出百度 OAuth 页面完成授权。
- 分页浏览百度网盘目录。
- 根据 `size`、`md5`、`server_mtime` 判断远端文件是否已同步或有更新。
- 启动完整同步任务。
- 对目录中的单个文件启动同步。
- 请求停止正在运行的同步任务。
- 查看当前任务和近期任务状态。
- 查看覆盖率日历。

本地模式下默认不需要鉴权。服务模式下，管理页面和管理 API 需要 `ADMIN_API_KEY`，页面会通过 `X-API-Key` 请求头发送管理 key。

## 鉴权

本地模式默认关闭查询鉴权：

```text
STOCK_DATA_SERVER_MODE=0
```

部署到服务器时建议开启服务模式：

```text
STOCK_DATA_SERVER_MODE=1
DATA_API_KEY=请填写强随机值
ADMIN_API_KEY=请填写另一个强随机值
```

查询接口支持以下任一种请求头：

```text
X-API-Key: <DATA_API_KEY>
Authorization: Bearer <DATA_API_KEY>
```

管理接口需要使用 `ADMIN_API_KEY`。

## 日志

日志默认写入：

```text
data/logs/YYYY-MM-DD.log
```

日志内容包括 FastAPI 请求耗时、同步任务开始和结束、百度下载和缓存决策、导入解析结果以及 Parquet 分区写入情况。日志格式器会脱敏 `access_token`、`refresh_token`、`app_secret`、`client_secret`、`api_key` 和 bearer token。

## 测试

运行普通测试：

```bash
pytest
```

真实百度网盘测试默认跳过，需要明确开启：

```bash
RUN_LIVE_BAIDU_TESTS=1 \
BAIDU_APP_KEY=你的应用Key \
BAIDU_APP_SECRET=你的应用Secret \
BAIDU_TOKEN_FILE=./baidu_token.json \
pytest tests/live
```

## 数据和凭证安全

以下内容属于本地运行产物或敏感信息，不应提交到 Git：

- `.env`
- `.env.*`
- `baidu_token.json`
- `data/`
- `logs/`
- `*.duckdb`
- `*.duckdb.wal`
- `*.log`
- `*.egg-info/`
- `__pycache__/`
- `.pytest_cache/`
- `.venv/`

仓库只保留 `.env.example` 作为配置模板。真实 API key、百度应用 secret、授权 token、运行数据库、Parquet 数据和日志都应保存在本机或服务器的安全配置系统中。
