#!/usr/bin/env python3
"""线上验证 pull 复制的追赶能力（leader/follower 完全按元数据推导）。

流程：建 topic → 生产 → 杀掉 p0 的 follower → 继续生产 → 重启该 follower
→ 验证它自动追平（副本字节数一致），且数据内容正确。
"""
import json
import os
import subprocess
import sys
import time
import urllib.parse
import urllib.request

BASE = "http://localhost:8080"
# 脚本用相对路径读取 data/，所以无论从哪儿调用都切到仓库根目录
os.chdir(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
TOPIC = sys.argv[1] if len(sys.argv) > 1 else "catchup-test"
failures = []


def post(path, payload):
    data = urllib.parse.urlencode(payload).encode()
    req = urllib.request.Request(BASE + path, data=data,
                                 headers={"Content-Type": "application/x-www-form-urlencoded"})
    try:
        with urllib.request.urlopen(req, timeout=90) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        return json.loads(e.read().decode())


def get(path):
    return json.loads(urllib.request.urlopen(BASE + path, timeout=10).read())


def check(what, ok, detail=""):
    print(("  [PASS] " if ok else "  [FAIL] ") + what + ("" if ok else "  (" + str(detail) + ")"))
    if not ok:
        failures.append(what)


def p0_assignment():
    """返回 (leader, [followers])；拿不到返回 (None, [])。"""
    meta = get("/api/metadata?port=9091")
    for t in meta["topics"]:
        if t["name"] == TOPIC:
            for p in t["partitions"]:
                if p["id"] == 0:
                    followers = [int(x) for x in p["followers"].strip("[]").split(",") if x.strip()]
                    return p["leader"], followers
    return None, []


def bytes_of(broker, partition=0):
    out = subprocess.check_output(
        ["bash", "-c", "ls -l data/%d/%s/%d/*.log 2>/dev/null | awk '{s+=$5} END {print s+0}'"
         % (broker, TOPIC, partition)]).decode().strip()
    return int(out or 0)


print("---- 1. 建 topic ----")
print("    createTopic ->", post("/api/topic", {"topic": TOPIC, "partitions": 3,
                                              "replicationFactor": 2, "port": 9091}))
time.sleep(2)
leader, followers = p0_assignment()
print("    partition 0: leader=%s followers=%s" % (leader, followers))
check("p0 有 leader 与恰好 1 个 follower", leader in (1, 2, 3) and len(followers) == 1,
      (leader, followers))
if failures:
    print("===> 前置条件不满足，终止")
    sys.exit(1)
follower = followers[0]

for i in range(5):
    post("/api/produce", {"topic": TOPIC, "partition": 0, "message": "before-%d" % i,
                          "port": str(9090 + leader)})
lb, fb = bytes_of(leader), bytes_of(follower)
for i in range(10):
    time.sleep(1)
    lb, fb = bytes_of(leader), bytes_of(follower)
    if lb == fb and lb > 0:
        break
print("    初始：leader(%d)=%dB follower(%d)=%dB" % (leader, lb, follower, fb))
check("初始复制已对齐（pull 复制生效）", lb == fb and lb > 0, (lb, fb))

print()
print("---- 2. 杀掉 follower(%d)，期间继续写入 ----" % follower)
print("    kill ->", post("/api/process", {"name": "broker-%d" % follower, "action": "kill"}))
time.sleep(2)
failed = 0
first_error = None
for i in range(20):
    res = post("/api/produce", {"topic": TOPIC, "partition": 0, "message": "while-down-%d" % i,
                                "port": str(9090 + leader)})
    if not res.get("ok"):
        failed += 1
        if first_error is None:
            first_error = res
check("follower 不在线时生产依然全部成功", failed == 0,
      "%d 条失败, 首个错误=%s" % (failed, first_error))
time.sleep(1)
lb, fb = bytes_of(leader), bytes_of(follower)
print("    leader=%dB follower=%dB（落后 %dB）" % (lb, fb, lb - fb))
check("follower 确实落后了", lb > fb, (lb, fb))

print()
print("---- 3. 重启 follower，验证自动追赶 ----")
print("    add broker-%d ->" % follower,
      post("/api/cluster/add", {"id": follower, "port": str(9090 + follower)}))
caught_up = False
for i in range(15):
    time.sleep(2)
    lb, fb = bytes_of(leader), bytes_of(follower)
    print("    t+%2ds leader=%dB follower=%dB" % ((i + 1) * 2, lb, fb))
    if lb == fb and lb > 0:
        caught_up = True
        break
check("重启后 follower 自动追平（字节数一致）", caught_up, (bytes_of(leader), bytes_of(follower)))

cur_leader, cur_followers = p0_assignment()
print("    当前分配: leader=%s followers=%s" % (cur_leader, cur_followers))
check("副本仍然是 2 份（没有被缩成单副本）", len(cur_followers) == 1, cur_followers)
check("被重启的节点仍在副本列表里（宽限期生效）", follower in cur_followers, (follower, cur_followers))

print()
print("---- 4. 数据内容一致 ----")
res = post("/api/fetch", {"topic": TOPIC, "partition": 0, "offset": 0, "port": str(9090 + cur_leader)})
msgs = res.get("messages", [])
check("能读到 25 条", len(msgs) == 25, len(msgs))
if msgs:
    check("首条与末条内容正确",
          msgs[0]["message"] == "before-0" and msgs[-1]["message"] == "while-down-19",
          (msgs[0]["message"], msgs[-1]["message"]))

state = get("/api/state")
for p in state["processes"]:
    if p["name"] == "broker-%d" % follower:
        lines = [l.strip() for l in p["output"].split("\n") if "Replica fetch" in l or "Truncated" in l]
        if lines:
            print("    追赶日志: " + lines[-1][:160])

print()
if failures:
    print("===> %d 项失败: %s" % (len(failures), failures))
    sys.exit(1)
print("===> pull 复制追赶验证通过")
