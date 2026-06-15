#!/usr/bin/env bash
# 演示 CRDT 不丢更新：多节点并发增量 counter 会求和（LWW 只会得 1），set 求并集。
set -u
BIN=target/debug/gossipdb
PIDS=()
cleanup() { for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null; done; wait 2>/dev/null; }
trap cleanup EXIT

start() { $BIN --id "$1" --bind "127.0.0.1:$2" --http "127.0.0.1:$3" --seeds "$4" >/tmp/gc-$1.log 2>&1 & PIDS+=($!); }

echo "== 启动 3 节点 =="
start 1 7101 8101 "127.0.0.1:7102,127.0.0.1:7103"
start 2 7102 8102 "127.0.0.1:7101,127.0.0.1:7103"
start 3 7103 8103 "127.0.0.1:7101,127.0.0.1:7102"
sleep 2

echo
echo "== Counter：三个节点各给 hits +1（若是 LWW 只会得 1）=="
curl -s -X POST 127.0.0.1:8101/counter/hits -d 1 >/dev/null
curl -s -X POST 127.0.0.1:8102/counter/hits -d 1 >/dev/null
curl -s -X POST 127.0.0.1:8103/counter/hits -d 1 >/dev/null
sleep 2
echo "  node1 hits = $(curl -s 127.0.0.1:8101/counter/hits)"
echo "  node2 hits = $(curl -s 127.0.0.1:8102/counter/hits)"
echo "  node3 hits = $(curl -s 127.0.0.1:8103/counter/hits)   <- 应为 3，不丢更新"

echo
echo "== Counter：node1 再 +5，node2 -2，最终一致 =="
curl -s -X POST 127.0.0.1:8101/counter/hits -d 5 >/dev/null
curl -s -X POST 127.0.0.1:8102/counter/hits -d -2 >/dev/null
sleep 2
echo "  三节点 hits = $(curl -s 127.0.0.1:8101/counter/hits) / $(curl -s 127.0.0.1:8102/counter/hits) / $(curl -s 127.0.0.1:8103/counter/hits)   <- 应为 6 (3+5-2)"

echo
echo "== Set：三节点各 add 不同标签 -> 并集 =="
curl -s -X POST 127.0.0.1:8101/set/tags/add -d radar >/dev/null
curl -s -X POST 127.0.0.1:8102/set/tags/add -d air >/dev/null
curl -s -X POST 127.0.0.1:8103/set/tags/add -d missile >/dev/null
sleep 2
echo "  node1 tags = $(curl -s 127.0.0.1:8101/set/tags)"
echo "  node3 tags = $(curl -s 127.0.0.1:8103/set/tags)   <- 应含 air/missile/radar 三个"

echo
echo "== Set：node2 移除 air，全集群收敛 =="
curl -s -X POST 127.0.0.1:8102/set/tags/remove -d air >/dev/null
sleep 2
echo "  node1 tags = $(curl -s 127.0.0.1:8101/set/tags)   <- air 应消失"

echo
echo "== /debug 看各 key 的 CRDT 类型 (node1) =="
curl -s 127.0.0.1:8101/debug | python3 -c "import sys,json;print(json.load(sys.stdin)['data'])"
echo
