# 网卡接收队列切换演练（FastAPI）

模拟网卡换接收队列时出现的乱序，验证上层仍然**每条流按收包顺序交付**。
所有操作都能在 `/docs` 里点着完成。

## 启动

### docker compose（推荐）

```bash
docker compose up --build
# 打开 http://localhost:8000/docs
```

可用环境变量：`PORT`（宿主端口，默认 8000）、`QUEUES`（队列数，默认 2）、
`MAX_PENDING_PER_FLOW`（每条流积压上限，默认 4）、`DELIVERY_SUMMARY_SIZE`
（每条流在内存里保留的交付摘要大小，默认 8）、`DELIVERY_STORE_PATH`
（交付记录库文件路径，容器里默认 `/data/deliveries.db`，本地直接跑默认
`./data/deliveries.db`）。

```bash
PORT=9000 QUEUES=3 MAX_PENDING_PER_FLOW=8 docker compose up --build
```

### 本地直接跑

```bash
python3 -m venv .venv && . .venv/bin/activate   # 可选
pip install -r requirements.txt
uvicorn app.main:app --reload
```

## 在 /docs 里怎么演练

1. `POST /packets`：送报文。填 `flow_id`、`seq`（包号）、`queue_id`（哪条队列收下它）。
   报文一旦被某条队列收下，就永远由这条队列交付。
2. `POST /switch`：选一条新队列接收**之后**的报文。已经收下的报文不会搬家。
3. `POST /queues/{queue_id}/advance`：单独推进某条队列，`count` 决定这次最多交付几个包。
   返回本次交给上层的报文和该队列剩余数量。
4. `GET /state`、`GET /queues`、`GET /egress?flow_id=`：查看积压分布和上层收到的报文顺序。
5. `POST /reset`：清空重来。

### 示例脚本（curl）

```bash
# 包 1 由队列 0 收下
curl -s -X POST localhost:8000/packets -H 'content-type: application/json' \
  -d '{"flow_id":"web","seq":1,"queue_id":0}'

# 换队列 1，后续包进队列 1
curl -s -X POST localhost:8000/switch -H 'content-type: application/json' \
  -d '{"queue_id":1}'
curl -s -X POST localhost:8000/packets -H 'content-type: application/json' \
  -d '{"flow_id":"web","seq":2,"queue_id":1}'

# 先推进新队列：包 2 被队列 0 上的包 1 挡住，交付为空
curl -s -X POST localhost:8000/queues/1/advance -H 'content-type: application/json' -d '{"count":10}'

# 另一条流不受影响，照常交付
curl -s -X POST localhost:8000/packets -H 'content-type: application/json' \
  -d '{"flow_id":"dns","seq":1,"queue_id":1}'
curl -s -X POST localhost:8000/queues/1/advance -H 'content-type: application/json' -d '{"count":10}'

# 推进原队列交付包 1，再推进新队列就能交付包 2
curl -s -X POST localhost:8000/queues/0/advance -H 'content-type: application/json' -d '{"count":10}'
curl -s -X POST localhost:8000/queues/1/advance -H 'content-type: application/json' -d '{"count":10}'
curl -s 'localhost:8000/egress?flow_id=web'
```

## 行为约定

| 规则 | 实现 |
| --- | --- |
| 已收的包由原队列处理 | 报文在接收时绑定 `queue_id`，切换只改之后新报文的去向 |
| 两边都有积压时也能再次切换 | `/switch` 可在任意时刻反复调用，不影响已有积压 |
| 每条流按收包顺序交付 | 流内按 `seq` 升序交付，队头是未交付报文里 `seq` 最小的 |
| 前序包没处理完时只让同一流等候 | 队头不在本队列的流整体跳过；其他流该交就交 |
| 每条流最多积压 4 个已接收未交付包 | `MAX_PENDING_PER_FLOW`，默认 4 |
| 满额只拒收这一流的新包 | 返回 503 + `{"reason":"backlog_full","rejected":true}`；其他流不受影响 |
| 腾出位置后可重试 | 推进队列交付一个包后，重送同一个包即可 |

重复包（已在积压或已交付）返回 409；包号小于已交付包号的过期包也返回 409；
队列编号越界返回 404；参数不合法返回 422。

## 交付记录落盘

跑久了内存不会涨：逐包交付记录（全局交付序号、流、包号、来源队列）整批写入
本地磁盘的 SQLite 库（`DELIVERY_STORE_PATH`），每条流在内存只留固定大小
（`DELIVERY_SUMMARY_SIZE`）的交付摘要。`/state`、`/egress`、`/flows/{id}`
等查询的字段、范围和顺序不变，只是数据改从磁盘读；`GET /egress?flow_id=`
仍只取该流的记录。判断重复包/过期包时，摘要窗口里找不到的老包会回查磁盘。

| 规则 | 实现 |
| --- | --- |
| 推进先写盘再改内存 | 一批交付记录事务落盘成功后，报文才从接收队列移除 |
| 写盘失败就报错 | 推进返回 500，整批包留在各自的接收队列，其他队列的同流后续包继续等候 |
| 恢复后可重试 | 重新推进即可，交付序号接着上次成功落盘的记录 |
| 未提交不影响查询 | 写盘尚未提交时，`/egress` 仍返回此前成功的交付记录 |
| 暂存状态有界 | 为恢复暂存的状态只随本批涉及的流和报文增长，写盘成功即清空 |
| 重置/重启清历史 | `/reset` 或进程重启后交付记录清空，交付序号从 1 重新开始 |

## 测试

```bash
pip install -r requirements.txt pytest httpx
python3 -m pytest -q
```

`tests/test_engine.py` 覆盖状态机，`tests/test_store.py` 覆盖交付记录落盘、
内存摘要与写盘失败恢复，`tests/test_api.py` 覆盖 HTTP 接口与 /docs。

## 目录

```
app/
  engine.py   核心状态机：接收、积压上限、按流保序交付
  store.py    逐包交付记录的本地磁盘存储（SQLite）
  models.py   Pydantic 模型（带 /docs 示例）
  main.py     FastAPI 路由
tests/
Dockerfile
docker-compose.yml
```
