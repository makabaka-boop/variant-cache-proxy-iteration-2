# 语言感知的缓存代理 + 可控源站

一个只处理 `GET /assets/{id}` 的 Go 反向缓存代理，配一个可以随时注入
故障 / 缓存策略 / 请求闸门的源站。核心目标：**一种语言的缓存绝不能交给
另一种语言**；源站偶尔返回过期或错误内容时，行为仍然可预期、可验收。

## 目录结构

```
cmd/origin/            可控源站进程（默认 :8081）
cmd/proxy/             缓存代理进程（默认 :8080）
internal/lang/         Accept-Language → zh/en 变体收敛（q 值排序）
internal/origin/       可控源站：资源配置、故障注入、闸门、分变体计数
internal/proxy/        缓存代理：可注入时钟、缓存判定、并发合并、陈旧兜底
scripts/accept.sh      针对运行中容器栈的端到端冒烟脚本
Dockerfile             runtime（代理/源站）与 verify（跑测试）三个目标
docker-compose.yml     origin、proxy 两个常驻服务 + 一次性 verify 服务
```

## 代理行为契约

- **只处理** `GET /assets/{id}`；其他路径 404，非 GET 返回 405（`Allow: GET`）。
- **可缓存条件**（必须同时满足，否则逐请求透传、不落缓存）：
  - 状态码 200；
  - 带 `ETag`；
  - `Cache-Control` 有明确 `max-age`，且 `0 ≤ max-age ≤ 60`（`no-cache` 按 0 处理）；
  - 无 `no-store` / `private`；
  - 显式声明 `Vary: Accept-Language`（缺失 Vary、`Vary: *` 或含其他字段
    一律不缓存——缓存键含语言，未显式声明语言差异的响应不值得冒串语言的风险）。
- **语言变体**：只支持 `zh`、`en`（按 RFC 7231 的 q 值选最高优先级受支持语言）。
  缓存键为 `(资源ID, 变体)`，两种变体各自独立的内容与 ETag。
  其余变体（如 `fr`、`ja`、`*`）响应强制补 `Cache-Control: no-store`，
  绝不缓存、绝不污染任何变体。
- **过期再验证**：新鲜期过后带 `If-None-Match` 回源；源站 304 时以
  **原响应的 max-age** 刷新新鲜期（`storedAt = now`，内容与 ETag 沿用，
  304 上更新的缓存头/ETag 一并采纳）。
- **并发合并**：同一资源、同一变体的并发请求只产生一次回源；等待者中途
  取消不会中断回源，也不影响其他等待者——回源使用脱离客户端生命周期的
  上下文，即便发起者断开，回源仍会完成并填充缓存。不同变体（zh/en）
  各自独立合并。
- **出错兜底**：回源网络错误或 5xx 时，若存在**已过期但不超过 30 秒**的
  旧条目，则返回旧内容（200）并显式标记：
  - `X-Cache-Status: stale`
  - `X-Cache-Stale: 1`
  - `Warning: 110 cacheproxy "response is stale"`
  - 另带 `X-Cache-Error: origin-500|origin-network-error|...`

  超出窗口或没有旧条目时返回 502（`X-Cache-Status: error`）。4xx 会作废旧条目。
- **stale-while-revalidate 后台刷新模式**（仅当源站在 `Cache-Control` 上
  **显式声明** `stale-while-revalidate=N` 时启用；未声明的资源一律保持上面的
  同步再验证/故障兜底行为）：
  - 非条件请求遇到“已过期但仍在该窗口内（`maxAge ≤ age ≤ maxAge+N`）”的条目时，
    **立即**返回完整旧内容，并打上与兜底一致的陈旧标记
    （`X-Cache-Status: stale`、`X-Cache-Stale: 1`、`Warning: 110`），
    但不带 `X-Cache-Error`（这不是故障兜底）；
  - 同时针对**同一资源、同一语言**只启动**一轮**后台**条件**回源
    （携带旧条目 ETag 的 `If-None-Match`）。同步轮与后台轮在同一把锁下原子互斥，
    任意时刻每键至多一轮回源；后台轮脱离任何客户端生命周期，发起者断开不影响它；
  - 后台结果语义：源站 **304 续期**（storedAt 刷新、内容/ETag 沿用，采纳 304 上
    更新的缓存头）；可缓存 **200 替换**条目；**4xx 按既有语义作废**旧条目；
    **网络错误/5xx 保留**仍可用的旧内容，下一个窗口内请求再试；
  - 后台结果提交前用指针比较确认它仍对应当前缓存条目；条目已被更新/删除时
    **迟到结果整体丢弃**，绝不会把旧内容重新写成新版本；
  - 窗口外（`age > maxAge+N`）恢复同步条件再验证。
- **客户端条件请求**：`If-None-Match` 只在**正确变体**的缓存条目上校验：
  - 新鲜命中且 ETag 匹配 → 304（`X-Cache-Status: hit`）；
  - **陈旧条目永不凭客户端校验器直接回 304**——即使 ETag 匹配也先回源条件
    验证（SWR 窗口内也不例外：条件请求不触发后台轮，而是同步等待/回源）；
  - 跨语言 ETag 不可能命中（zh 的 ETag 去问 en 返回完整 200）；
  - 本地无缓存时携带客户端校验器透传回源（200 可正常入缓存）。
- 新鲜度边界：`age == max-age` 即过期；`age == max-age + 30s` 仍可兜底，
  再多 1 秒则 502。SWR 窗口同理：`age == max-age + swr` 仍可立即返回旧内容，
  再多 1 秒恢复同步再验证。
- 可观测响应头 `X-Cache-Status`：`miss | hit | revalidated | stale | bypass | error`，
  有缓存的响应还带 `Age`。

## 前置条件

- Docker 24+ 与 Docker Compose v2；或本地 Go 1.22+（仅跑测试时需要）。

## 启动（Docker Compose）

```bash
docker compose up --build -d
# 等价：make up

curl -sS -i http://localhost:8080/assets/demo -H 'Accept-Language: zh'
curl -sS -i http://localhost:8080/assets/demo -H 'Accept-Language: en'
```

- 代理：http://localhost:8080 （仅 `/assets/{id}`）
- 可控源站：http://localhost:8081

停止：`docker compose down -v`（或 `make down`）。

## 验收命令

### 1. 一次性自动测试服务（完整测试，含竞态检测）

```bash
docker compose run --rm verify
# 等价：make verify
```

`verify` 是一次性服务：容器内执行 `go test -race -count=1 -v ./...`，
退出码 0 即全部通过。测试用真实 HTTP 端到端驱动（httptest 起源站与代理），
时钟与源站行为均为注入式；除协调等待外不依赖真实时序。

本地有 Go 时也可直接：

```bash
go test -race -count=1 ./...   # 或 make test-race
```

测试覆盖：

- 并发合并：闸门挂起源站，断言 8 个同键请求只有 1 次回源、在飞峰值为 1；
  zh/en 同时并发时各 1 次、峰值为 2；
- 取消：等待者取消后快速返回、不触发额外回源，其他等待者正常拿到结果；
  发起者（leader）取消后回源仍完成，后来加入者复用同一轮；SWR 触发请求
  结束后，被闸门挂住的后台轮仍自行完成并填充缓存；
- 过期边界：`maxAge`、`maxAge+30s`、`maxAge+31s` 三个边界点的
  hit / stale / 502 行为；源站 304 续期后的新鲜窗口；
- stale-while-revalidate：`maxAge`、`maxAge+swr`、`maxAge+swr+1s` 的
  stale / stale / 同步再验证边界；窗口内显式陈旧标记（无 `X-Cache-Error`）；
  每 (资源, 变体) 只有一轮后台条件回源；后台 304 续期、可缓存 200 替换、
  4xx 作废、5xx 与网络错误保留旧内容；迟到提交（条目已前进）结果被丢弃；
  客户端 If-None-Match 在陈旧条目上不直接 304；未声明指令的资源保持旧行为；
- 跨语言隔离：zh/en 独立缓存、独立 ETag，跨语言条件请求不 304，
  stale 兜底也不会串语言；双语言后台刷新各自独立（峰值为 2）；
- 可缓存性矩阵：max-age 0/60/61、缺 ETag、`Vary: *`、多字段 Vary、no-store、
  stale-while-revalidate 合法/非法写法等；
- 不支持变体 `no-store`、冷条件透传、路由 404/405、源站控制面冒烟。

### 2. 端到端冒烟脚本（对运行中的栈）

```bash
docker compose up --build -d
sh ./scripts/accept.sh         # 或 make smoke
```

脚本会真实演练：miss→hit、跨语言 ETag 不 304 / 同语言 304、
不支持变体 no-store、过期后 500 返回带 `Warning 110` 的陈旧内容、
源站恢复后 304 续期、闸门下 8 并发只产生 1 次回源。

## 可控源站使用说明

| 接口 | 方法 | 作用 |
| --- | --- | --- |
| `/assets/{id}` | GET | 资源接口，按 `Accept-Language` 返回 zh/en，带分变体 ETag |
| `/control/reset` | POST | 清空分变体计数 |
| `/control/assets/{id}` | POST | 配置资源（JSON，字段均可选） |
| `/control/stats` | GET | 查看 `total/status/inFlight/maxInFlight` |
| `/control/gate/close` | GET | 拦截所有资源请求直到放行 |
| `/control/gate/open` | GET | 放行 |

配置示例：

```bash
# max-age=5 的正常资源
curl -X POST http://localhost:8081/control/assets/demo \
  -H 'Content-Type: application/json' \
  -d '{"maxAge":5,"hasETag":true,"vary":"Accept-Language"}'

# 显式启用 stale-while-revalidate=15（SWR 后台刷新模式）
curl -X POST http://localhost:8081/control/assets/demo \
  -H 'Content-Type: application/json' \
  -d '{"maxAge":5,"swr":15}'

# 把内容升级到 v2（ETag/正文随之变化，确定性制造条件回源 200）
curl -X POST http://localhost:8081/control/assets/demo \
  -H 'Content-Type: application/json' -d '{"version":2}'

# 注入 500 故障（演练 stale 兜底）
curl -X POST http://localhost:8081/control/assets/demo \
  -H 'Content-Type: application/json' -d '{"fail":true}'

# 注入 404（演练 4xx 失效语义）
curl -X POST http://localhost:8081/control/assets/demo \
  -H 'Content-Type: application/json' -d '{"status":404}'

# 让响应不可缓存（缺 ETag / Vary 不兼容 / max-age 越界）
curl -X POST http://localhost:8081/control/assets/demo \
  -H 'Content-Type: application/json' -d '{"hasETag":false}'

# 观察源站实际收到的请求数（分 zh/en/other，含 200/304/500 明细）
curl -s http://localhost:8081/control/stats
```

## 设计要点

- **变体收敛在源站与代理共用同一个包**（`internal/lang`），两端对
  `Accept-Language` 的解释完全一致，从根上避免“代理认为是 A、源站按 B 应答”。
- **回源请求只发送单一语言标签**（`zh` 或 `en`），源站返回什么变体完全确定，
  不信任源站可能出错的 `Content-Language` 来决定缓存键。
- 合并原语（`internal/proxy/round.go`）用每键一个 `round` 统一同步回源与
  SWR 后台刷新：两种轮次在同一把锁下原子互斥（任意时刻每键至多一轮），
  同步等待者可中途放弃，后台轮与同步 leader 都用
  `context.WithoutCancel` 脱离请求生命周期，提交缓存后再关闭通道通知等待者。
  后台/同步结论提交都做“条目指针仍是发起依据”的 CAS 校验，迟到结果让位。
- 时间只来自可注入的 `Clock` 接口；代理没有后台清理 goroutine，
  过期判断全部发生在请求路径上。SWR 窗口口径（解析器、源站指令、缓存条目、
  HTTP 响应）共用同一个 delta-seconds 值。
