#!/usr/bin/env python3
"""线上验证保留策略（用独立的小集群，不影响 2181 上的主集群）。

自建：ZK(2281) + broker-1(9191) + broker-2(9192)，工作目录 /tmp/retention-test，
broker 用 -Dsimplekafka.segment.bytes=16384 -Dsimplekafka.retention.bytes=65536 启动。
用**原始协议**直接与 broker 对话，不依赖控制台。
"""
import json
import os
import shutil
import signal
import socket
import struct
import subprocess
import sys
import time

JAR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
                   "target", "simple-kafka-1.0-SNAPSHOT.jar")
WORK = "/tmp/retention-test"
ZK_PORT = 2281
BROKERS = {1: 9191, 2: 9192}
TOPIC = "ret-test"
failures = []


def check(what, ok, detail=""):
    print(("  [PASS] " if ok else "  [FAIL] ") + what + ("" if ok else "  (" + str(detail) + ")"))
    if not ok:
        failures.append(what)


def wait_port(port, timeout=25):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), 1):
                return True
        except OSError:
            time.sleep(0.3)
    return False


def recv_exact(f, n):
    data = f.read(n)
    if data is None or len(data) < n:
        raise IOError("connection closed, wanted %d got %s" % (n, "None" if data is None else len(data)))
    return data


def create_topic(port, topic, partitions, rf):
    req = bytes([0x04]) + struct.pack(">H", len(topic)) + topic.encode() + \
        struct.pack(">i", partitions) + struct.pack(">h", rf)
    with socket.create_connection(("127.0.0.1", port), 5) as s:
        f = s.makefile("rb")
        s.sendall(req)
        resp = recv_exact(f, 2)
        return resp[0] == 0x14 and resp[1] == 0


def produce(port, topic, partition, payload):
    req = bytes([0x01]) + struct.pack(">H", len(topic)) + topic.encode() + \
        struct.pack(">i", partition) + struct.pack(">i", len(payload)) + payload
    with socket.create_connection(("127.0.0.1", port), 5) as s:
        f = s.makefile("rb")
        s.sendall(req)
        resp = recv_exact(f, 10)
        if resp[0] != 0x11:
            return None
        return struct.unpack(">q", resp[1:9])[0]


def fetch(port, topic, partition, offset, max_bytes=65536):
    """返回 (records, error)：records 是 [(offset, payload)]。"""
    req = bytes([0x02]) + struct.pack(">H", len(topic)) + topic.encode() + \
        struct.pack(">i", partition) + struct.pack(">q", offset) + struct.pack(">i", max_bytes)
    with socket.create_connection(("127.0.0.1", port), 5) as s:
        f = s.makefile("rb")
        s.sendall(req)
        first = recv_exact(f, 1)[0]
        if first == 0x1F:
            length = struct.unpack(">h", recv_exact(f, 2))[0]
            return [], recv_exact(f, length).decode()
        if first != 0x12:
            return [], "unexpected response type 0x%02x" % first
        count = struct.unpack(">i", recv_exact(f, 4))[0]
        records = []
        for _ in range(count):
            off, size = struct.unpack(">qi", recv_exact(f, 12))
            records.append((off, recv_exact(f, size)))
        return records, None


def segment_files(broker, partition=0):
    d = os.path.join(WORK, "data", str(broker), TOPIC, str(partition))
    if not os.path.isdir(d):
        return []
    return sorted(f for f in os.listdir(d) if f.endswith(".log"))


def total_bytes(broker, partition=0):
    d = os.path.join(WORK, "data", str(broker), TOPIC, str(partition))
    if not os.path.isdir(d):
        return 0
    return sum(os.path.getsize(os.path.join(d, f)) for f in os.listdir(d) if f.endswith(".log"))


procs = []
try:
    shutil.rmtree(WORK, ignore_errors=True)
    os.makedirs(WORK, exist_ok=True)
    cfg = os.path.join(WORK, "zk.cfg")
    with open(cfg, "w") as f:
        f.write("tickTime=2000\ndataDir=%s/zkdata\nclientPort=%d\nmaxClientCnxns=60\nadmin.enableServer=false\n"
                % (WORK, ZK_PORT))

    print("---- 启动独立小集群（ZK:%d, broker:%s）----" % (ZK_PORT, list(BROKERS.values())))
    procs.append(subprocess.Popen(["java", "-cp", JAR, "org.apache.zookeeper.server.quorum.QuorumPeerMain", cfg],
                                  cwd=WORK, stdout=open(WORK + "/zk.log", "w"), stderr=subprocess.STDOUT))
    check("ZooKeeper 就绪", wait_port(ZK_PORT), ZK_PORT)

    for bid, port in BROKERS.items():
        cmd = ["java",
               "-Dsimplekafka.segment.bytes=16384",
               "-Dsimplekafka.retention.bytes=65536",
               "-Dsimplekafka.retention.ms=600000",
               "-cp", JAR, "com.simplekafka.broker.SimpleKafkaBroker",
               str(bid), "localhost", str(port), str(ZK_PORT)]
        procs.append(subprocess.Popen(cmd, cwd=WORK, stdout=open(WORK + "/broker%d.log" % bid, "w"),
                                      stderr=subprocess.STDOUT))
    for bid, port in BROKERS.items():
        check("broker-%d 就绪" % bid, wait_port(port), port)
    time.sleep(3)

    print()
    print("---- 1. 建 topic + 生产，验证分片与复制 ----")
    check("createTopic(%s, 3, 2)" % TOPIC, create_topic(BROKERS[1], TOPIC, 3, 2))
    time.sleep(2)

    count = 40
    payload = "R" * 2048
    last = None
    for i in range(count):
        last = produce(BROKERS[1], TOPIC, 0, ("%04d-" % i).encode() + payload.encode())
        if last is None:
            break
    check("成功生产 %d 条 2KB 消息" % count, last == count - 1, last)

    time.sleep(3)
    leader_bytes = total_bytes(1, 0)
    follower_bytes = total_bytes(2, 0)
    print("    生产后：leader=%dB follower=%dB 段数=%d" % (leader_bytes, follower_bytes, len(segment_files(1, 0))))
    check("follower 通过 pull 复制追上了（字节数一致）", leader_bytes == follower_bytes and leader_bytes > 0,
          (leader_bytes, follower_bytes))
    check("生产量已超过保留上限 65536", leader_bytes > 65536, leader_bytes)

    print()
    print("---- 2. 等待保活线程执行保留策略清理 ----")
    before_files = len(segment_files(1, 0))
    time.sleep(9)
    after_files = len(segment_files(1, 0))
    after_bytes = total_bytes(1, 0)
    print("    leader 段数 %d → %d，字节 %d → %d" % (before_files, after_files, leader_bytes, after_bytes))
    check("旧段被删除（段数下降）", after_files < before_files, (before_files, after_files))
    check("清理后仍在保留上限附近", after_bytes <= 65536 + 16384, after_bytes)
    check("至少保留一个段", after_files >= 1, after_files)

    print()
    print("---- 3. 被删除的 offset 必须明确报错，而不是错位返回 ----")
    records, error = fetch(BROKERS[1], TOPIC, 0, 0)
    print("    fetch(offset=0) -> error=%r records=%d" % (error, len(records)))
    check("读取已被清理的 offset 返回错误", error is not None and "before log start" in error, error)

    # 找出真正的 logStart：从小往大试探
    log_start = None
    for probe in range(0, count):
        got, err = fetch(BROKERS[1], TOPIC, 0, probe)
        if err is None and got:
            log_start = probe
            break
    print("    探测到的 logStart = %s" % log_start)
    check("logStart 前移（老 offset 不可读）", log_start is not None and log_start > 0, log_start)

    records, error = fetch(BROKERS[1], TOPIC, 0, log_start, 65536)
    check("从 logStart 起可正常读取", error is None and len(records) > 0, error)
    if records:
        expected_prefix = ("%04d-" % log_start).encode()
        check("logStart 处的记录 offset 与内容正确",
              records[0][0] == log_start and records[0][1].startswith(expected_prefix),
              (records[0][0], records[0][1][:8]))

    print()
    print("---- 4. follower 的起点也对齐到 leader 的 logStart ----")
    time.sleep(6)
    follower_files = segment_files(2, 0)
    leader_files = segment_files(1, 0)
    print("    leader 段 %s" % leader_files)
    print("    follower 段 %s" % follower_files)
    check("follower 已丢弃被 leader 删掉的老段",
          (follower_files[0] if follower_files else None) == (leader_files[0] if leader_files else "x"),
          (leader_files[:1], follower_files[:1]))
    check("副本字节数仍然一致", total_bytes(1, 0) == total_bytes(2, 0),
          (total_bytes(1, 0), total_bytes(2, 0)))

    print()
    print("---- 5. 清理后仍能继续写入与消费 ----")
    off = produce(BROKERS[1], TOPIC, 0, b"after-retention")
    check("清理后仍可写入", off is not None, off)
    time.sleep(2)
    records, error = fetch(BROKERS[1], TOPIC, 0, off)
    check("新写入的消息可读取", error is None and records and records[0][1] == b"after-retention",
          (error, records[:1]))
finally:
    print()
    print("---- 清理测试进程 ----")
    for p in procs:
        try:
            p.send_signal(signal.SIGTERM)
        except Exception:
            pass
    time.sleep(2)
    for p in procs:
        try:
            p.kill()
        except Exception:
            pass

print()
if failures:
    print("===> %d 项失败: %s" % (len(failures), failures))
    sys.exit(1)
print("===> 保留策略线上验证通过")
