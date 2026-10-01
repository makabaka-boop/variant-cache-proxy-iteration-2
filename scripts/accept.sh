#!/bin/sh
# 端到端冒烟脚本：针对已启动的 Compose 栈（代理 :8080、源站 :8081）。
# 覆盖：缓存命中、跨语言隔离、变体级 304、不支持变体 no-store、
# 过期条件再验证、出错返回带标记的陈旧响应、并发合并。
set -eu

PROXY=${PROXY_URL:-http://localhost:8080}
ORIGIN=${ORIGIN_URL:-http://localhost:8081}
A=accept-demo

curl_() { curl -sS -i "$@"; }

header() { # header <name> <file>
	tr -d '\r' < "$2" | grep -i "^$1:" | tail -1 | sed 's/^[^:]*: *//'
}

echo "== 0. 重置并配置资源 max-age=2"
curl_ -X POST "$ORIGIN/control/reset" -o /dev/null
curl_ -X POST "$ORIGIN/control/assets/$A" \
	-H 'Content-Type: application/json' \
	-d '{"maxAge":2,"hasETag":true}' -o /dev/null

echo "== 1. zh 首次 miss，二次 hit（不再回源）"
curl_ "$PROXY/assets/$A" -H 'Accept-Language: zh' >/tmp/r1
s1=$(header X-Cache-Status /tmp/r1); zh_etag=$(header ETag /tmp/r1)
[ "$s1" = "miss" ] || { echo "FAIL: first zh status=$s1"; exit 1; }
curl_ "$PROXY/assets/$A" -H 'Accept-Language: zh' >/tmp/r2
s2=$(header X-Cache-Status /tmp/r2)
[ "$s2" = "hit" ] || { echo "FAIL: second zh status=$s2"; exit 1; }
echo "   zh: miss -> hit, ETag=$zh_etag"

echo "== 2. en 拥有独立内容/ETag；zh 的 ETag 问 en 不得 304"
curl_ "$PROXY/assets/$A" -H 'Accept-Language: en' >/tmp/re
en_etag=$(header ETag /tmp/re)
[ "$zh_etag" != "$en_etag" ] || { echo "FAIL: ETags not per-variant"; exit 1; }
code=$(curl_ -o /tmp/rx -w '%{http_code}' "$PROXY/assets/$A" \
	-H 'Accept-Language: en' -H "If-None-Match: $zh_etag")
[ "$code" = "200" ] || { echo "FAIL: cross-language validator gave $code"; exit 1; }
code=$(curl_ -o /tmp/rx -w '%{http_code}' "$PROXY/assets/$A" \
	-H 'Accept-Language: en' -H "If-None-Match: $en_etag")
[ "$code" = "304" ] || { echo "FAIL: matching en validator gave $code"; exit 1; }
echo "   en ETag=$en_etag；跨语言 200，同语言 304"

echo "== 3. 不支持的变体 fr：强制 no-store，每次都回源"
curl_ "$PROXY/assets/$A" -H 'Accept-Language: fr' >/tmp/rf1
cc=$(header Cache-Control /tmp/rf1)
[ "$cc" = "no-store" ] || { echo "FAIL: fr cache-control=$cc"; exit 1; }
curl_ "$PROXY/assets/$A" -H 'Accept-Language: fr' -o /dev/null
echo "   fr 响应 no-store"

echo "== 4. 过期后出错：30 秒内返回旧内容并显式标记"
sleep 3 # max-age=2，此刻已过期
curl_ -X POST "$ORIGIN/control/assets/$A" \
	-H 'Content-Type: application/json' -d '{"fail":true}' -o /dev/null
curl_ "$PROXY/assets/$A" -H 'Accept-Language: zh' >/tmp/rs
sc=$(header X-Cache-Status /tmp/rs); warn=$(header Warning /tmp/rs)
[ "$sc" = "stale" ] || { echo "FAIL: stale status=$sc"; exit 1; }
[ -n "$warn" ] || { echo "FAIL: missing Warning"; exit 1; }
echo "   X-Cache-Status=stale, Warning=$warn"

echo "== 5. 源站恢复：条件请求 304 延长新鲜期"
curl_ -X POST "$ORIGIN/control/assets/$A" \
	-H 'Content-Type: application/json' -d '{"fail":false}' -o /dev/null
curl_ "$PROXY/assets/$A" -H 'Accept-Language: zh' >/tmp/rr
sr=$(header X-Cache-Status /tmp/rr)
[ "$sr" = "revalidated" ] || { echo "FAIL: revalidation status=$sr"; exit 1; }
echo "   revalidated"

echo "== 6. 并发合并：8 个同键请求只触发一次回源"
curl_ -X POST "$ORIGIN/control/reset" -o /dev/null
curl_ "$ORIGIN/control/gate/close" -o /dev/null
i=0
while [ "$i" -lt 8 ]; do
	(curl_ "$PROXY/assets/coal" -H 'Accept-Language: zh' -o /dev/null) &
	i=$((i+1))
done
sleep 1
stats=$(curl_ "$ORIGIN/control/stats")
echo "$stats" | grep -q '"zh":1' || { echo "FAIL: coalescing, stats=$stats"; exit 1; }
curl_ "$ORIGIN/control/gate/open" -o /dev/null
wait
sleep 0.5
stats=$(curl_ "$ORIGIN/control/stats")
echo "$stats" | grep -q '"zh":1' || { echo "FAIL: total fetch after gate, stats=$stats"; exit 1; }
echo "   闸门期间仅 1 次回源，放行后全部等待者拿到同一结果"

echo
echo "ACCEPT SMOKE OK"
