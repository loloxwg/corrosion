#!/usr/bin/env bash
# 3 进程收敛演示，验证 DoD 三条：扩散收敛 / 冲突裁决 / 故障容忍+追平。
set -u
BIN=target/debug/gossipdb
PIDS=()

cleanup() {
  for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null; done
  wait 2>/dev/null
}
trap cleanup EXIT

start_node() {
  local id=$1 udp=$2 http=$3 seeds=$4
  $BIN --id "$id" --bind "127.0.0.1:$udp" --http "127.0.0.1:$http" --seeds "$seeds" \
    >/tmp/gossipdb-$id.log 2>&1 &
  PIDS+=($!)
}

get() { curl -s "127.0.0.1:$1/kv/$2"; }
put() { curl -s -X PUT "127.0.0.1:$1/kv/$2" -d "$3" >/dev/null; }

echo "== 启动 3 节点 =="
start_node 1 7001 8001 "127.0.0.1:7002,127.0.0.1:7003"
start_node 2 7002 8002 "127.0.0.1:7001,127.0.0.1:7003"
start_node 3 7003 8003 "127.0.0.1:7001,127.0.0.1:7002"
sleep 2

echo
echo "== DoD1 扩散收敛：node1 写 color=red，读 node2/node3 =="
put 8001 color red
sleep 2
echo "  node2 color = $(get 8002 color)"
echo "  node3 color = $(get 8003 color)"

echo
echo "== DoD2 冲突裁决：node1 写 x=A 与 node2 写 x=B 几乎同时 =="
put 8001 x A
put 8002 x B
sleep 2
v1=$(get 8001 x); v2=$(get 8002 x); v3=$(get 8003 x)
echo "  node1 x = $v1"
echo "  node2 x = $v2"
echo "  node3 x = $v3"
if [ "$v1" = "$v2" ] && [ "$v2" = "$v3" ]; then
  echo "  -> 三节点一致 ($v1) ✓"
else
  echo "  -> 不一致 ✗"
fi

echo
echo "== DoD3 故障容忍：杀掉 node3，node1/2 继续读写 =="
kill "${PIDS[2]}" 2>/dev/null
PIDS=("${PIDS[0]}" "${PIDS[1]}")
sleep 1
put 8001 mood happy
sleep 1
echo "  node2 mood = $(get 8002 mood)  (集群在缺一个节点时仍工作)"

echo
echo "== node3 重启，通过对账追平历史数据 =="
start_node 3 7003 8003 "127.0.0.1:7001,127.0.0.1:7002"
sleep 4
echo "  node3 color = $(get 8003 color)  (重启前写入的)"
echo "  node3 x     = $(get 8003 x)"
echo "  node3 mood  = $(get 8003 mood)   (node3 宕机期间写入的)"

echo
echo "== 成员视图 (node1 视角) =="
curl -s 127.0.0.1:8001/members
echo
