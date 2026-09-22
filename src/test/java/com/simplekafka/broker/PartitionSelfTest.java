package com.simplekafka.broker;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Partition 第一期持久化改造自测：①崩溃恢复/截断脏尾 ③跨段读取 ②索引重建 ④稀疏索引 ⑤段轮转。
 * 直接调用 Partition，不依赖 ZooKeeper / 网络。
 */
public class PartitionSelfTest {

    private static int failures = 0;

    public static void main(String[] args) throws Exception {
        File base = new File("/tmp/partition-selftest-data");
        deleteRecursively(base);

        testCrossSegmentAndSparseIndex(new File(base, "cross"));
        testDirtyTailIncompletePayload(new File(base, "dirty-payload"));
        testDirtyTailHalfHeader(new File(base, "dirty-header"));
        testGarbageLengthHeader(new File(base, "dirty-length"));
        testReplicationIdempotency(new File(base, "replica"));
        testRetentionByBytes(new File(base, "retention-bytes"));
        testRetentionByAge(new File(base, "retention-age"));
        testTruncateBefore(new File(base, "truncate"));

        System.out.println();
        System.out.println(failures == 0 ? "===> ALL TESTS PASSED" : "===> " + failures + " TEST(S) FAILED");
        System.exit(failures == 0 ? 0 : 1);
    }

    // ------------------------------------------------------------------
    // ① 保留策略：按字节 / 按时间清理旧段
    // ------------------------------------------------------------------
    private static void testRetentionByBytes(File dir) throws Exception {
        section("① 保留策略：按字节清理旧段");
        System.out.println("    segment.bytes=" + Partition.MAX_SEGMENT_BYTES
                + " retention.bytes=" + Partition.RETENTION_BYTES
                + " retention.ms=" + Partition.RETENTION_MS);
        if (Partition.RETENTION_BYTES <= 0) {
            System.out.println("  [SKIP] 未启用 retention.bytes");
            return;
        }

        Partition p = new Partition(0, 1, new ArrayList<>(), dir.getAbsolutePath());
        long target = Math.max(Partition.RETENTION_BYTES * 2, Partition.MAX_SEGMENT_BYTES * 3);
        int written = 0;
        while (p.getTotalSize() < target && written < 20000) {
            p.append(message(written++));
        }
        long sizeBefore = p.getTotalSize();
        int segmentsBefore = p.getSegmentCount();
        System.out.println("    写入 " + written + " 条, 总大小 " + sizeBefore + "B, 段数 " + segmentsBefore);
        check("写入量超过保留上限", sizeBefore > Partition.RETENTION_BYTES, sizeBefore);

        int deleted = p.cleanup();
        System.out.println("    删除 " + deleted + " 段, 剩余 " + p.getTotalSize()
                + "B, 段数 " + p.getSegmentCount() + ", logStart=" + p.getLogStartOffset());
        check("确实删除了旧段", deleted > 0, deleted);
        check("至少保留一个段", p.getSegmentCount() >= 1, p.getSegmentCount());
        check("清理后大小收敛到上限附近",
                p.getTotalSize() <= Partition.RETENTION_BYTES + Partition.MAX_SEGMENT_BYTES, p.getTotalSize());
        check("logStartOffset 前移", p.getLogStartOffset() > 0, p.getLogStartOffset());

        long logStart = p.getLogStartOffset();
        boolean threw = false;
        try {
            p.readMessages(logStart - 1, 4096);
        } catch (IllegalArgumentException e) {
            threw = true;
        }
        check("读取已被删除的 offset 会明确报错（不会错位返回新数据）", threw, "未抛异常");

        List<byte[]> fromStart = p.readMessages(logStart, 64 * 1024);
        check("从 logStartOffset 起能正常读取", !fromStart.isEmpty(), fromStart.size());
        check("logStartOffset 处内容正确",
                !fromStart.isEmpty() && Arrays.equals(fromStart.get(0), message((int) logStart)),
                fromStart.isEmpty() ? "empty" : describe(fromStart.get(0)));

        long leo = p.getLogEndOffset();
        check("清理后继续追加 offset 连续", p.append(message((int) leo)) == leo, leo);
        p.close();
    }

    private static void testRetentionByAge(File dir) throws Exception {
        section("① 保留策略：按时间清理旧段");
        if (Partition.RETENTION_MS <= 0) {
            System.out.println("  [SKIP] 未启用 retention.ms");
            return;
        }

        Partition p = new Partition(0, 1, new ArrayList<>(), dir.getAbsolutePath());
        int written = 0;
        while (p.getSegmentCount() < 2 && written < 20000) {
            p.append(message(written++));
        }
        check("已产生多个段", p.getSegmentCount() >= 2, p.getSegmentCount());

        File oldest = filesWithSuffix(dir, ".log").get(0);
        long past = System.currentTimeMillis() - Partition.RETENTION_MS - 60_000L;
        check("把最旧段的时间改到很久以前", oldest.setLastModified(past), oldest.getName());

        int deleted = p.cleanup();
        check("按时间删除了过期段", deleted >= 1, deleted);
        check("过期段文件已从磁盘删除", !oldest.exists(), oldest.getAbsolutePath());
        check("logStartOffset 前移", p.getLogStartOffset() > 0, p.getLogStartOffset());
        check("活跃段没有被删除", p.getSegmentCount() >= 1, p.getSegmentCount());
        check("仍能读到数据", !p.readMessages(p.getLogStartOffset(), 64 * 1024).isEmpty(), "");
        p.close();
    }

    // ------------------------------------------------------------------
    // ② follower 起点对齐：truncateBefore
    // ------------------------------------------------------------------
    private static void testTruncateBefore(File dir) throws Exception {
        section("② follower 起点对齐（truncateBefore）");

        Partition p = new Partition(0, 1, new ArrayList<>(), dir.getAbsolutePath());
        int written = 0;
        while (p.getSegmentCount() < 3 && written < 20000) {
            p.append(message(written++));
        }
        check("已产生 3 个段", p.getSegmentCount() >= 3, p.getSegmentCount());

        List<File> logs = filesWithSuffix(dir, ".log");
        long boundary = Long.parseLong(logs.get(2).getName().replace(".log", ""));
        long leoBefore = p.getLogEndOffset();

        p.truncateBefore(boundary);
        System.out.println("    起点对齐到 " + boundary + "，剩余 " + filesWithSuffix(dir, ".log").size() + " 个段");
        check("logStartOffset 对齐到目标段起点", p.getLogStartOffset() == boundary, p.getLogStartOffset());
        check("对齐后 LEO 不变", p.getLogEndOffset() == leoBefore, p.getLogEndOffset());
        check("被丢弃的 offset 报错", throwsOnRead(p, boundary - 1), "未抛异常");
        check("对齐点处可读", !p.readMessages(boundary, 64 * 1024).isEmpty(), "");

        // 落后太多（leader 的起点已经超过自己的 LEO）→ 整个分区重置并以该 offset 作为新起点
        long resetTo = p.getLogEndOffset() + 500;
        p.truncateBefore(resetTo);
        check("完全落后时 LEO 被重置到新起点", p.getLogEndOffset() == resetTo, p.getLogEndOffset());
        check("完全落后时 logStartOffset 等于新起点", p.getLogStartOffset() == resetTo, p.getLogStartOffset());
        check("重置后旧 offset 报错", throwsOnRead(p, resetTo - 1), "未抛异常");
        long appended = p.append(message(0));
        check("重置后可以继续追加", appended == resetTo, appended);
        p.close();
    }

    private static boolean throwsOnRead(Partition p, long offset) {
        try {
            p.readMessages(offset, 4096);
            return false;
        } catch (IllegalArgumentException e) {
            return true;
        }
    }

    // ------------------------------------------------------------------
    // ③ 跨段读取 + ④ 稀疏索引 + ⑤ 段轮转
    // ------------------------------------------------------------------
    private static void testCrossSegmentAndSparseIndex(File dir) throws Exception {
        section("③④⑤ 跨段读取 / 稀疏索引 / 段轮转");

        Partition p = new Partition(0, 1, new ArrayList<>(), dir.getAbsolutePath());
        int count = 120;
        for (int i = 0; i < count; i++) {
            long offset = p.append(message(i));
            check("append 返回连续 offset", offset == i, "i=" + i + " offset=" + offset);
        }
        check("LEO = " + count, p.getLogEndOffset() == count, "LEO=" + p.getLogEndOffset());

        List<File> logs = filesWithSuffix(dir, ".log");
        List<File> indexes = filesWithSuffix(dir, ".index");
        System.out.println("  段数量 = " + logs.size() + " -> " + names(logs));
        check("写入 1.2MB 后产生多个段", logs.size() >= 2, "segments=" + logs.size());
        check("段数量与段文件数量一致", logs.size() == indexes.size(), "log=" + logs.size() + " index=" + indexes.size());

        for (File log : logs) {
            long size = log.length();
            check("段 " + log.getName() + " 未超过上限", size <= Partition.MAX_SEGMENT_BYTES,
                    "size=" + size + " max=" + Partition.MAX_SEGMENT_BYTES);
        }

        // 连续读：每批 64KB，直到读完，校验 offset 连续、内容正确
        long expectedOffset = 0;
        long batches = 0;
        while (true) {
            List<byte[]> batch = p.readMessages(expectedOffset, 64 * 1024);
            if (batch.isEmpty()) {
                break;
            }
            batches++;
            for (byte[] m : batch) {
                if (!Arrays.equals(m, message((int) expectedOffset))) {
                    check("offset " + expectedOffset + " 内容正确", false, "payload mismatch");
                }
                expectedOffset++;
            }
            if (batches > 1000) {
                break;
            }
        }
        check("从 0 读到全部 " + count + " 条", expectedOffset == count, "read=" + expectedOffset);
        System.out.println("  分批读取次数 = " + batches);

        // 跨段边界读：取第 2 个段的 baseOffset，从它前一条开始读
        long boundary = Long.parseLong(logs.get(1).getName().replace(".log", ""));
        System.out.println("  第二个段 baseOffset = " + boundary);
        check("段边界 offset 合法", boundary > 0 && boundary < count, "boundary=" + boundary);

        List<byte[]> across = p.readMessages(boundary - 1, 64 * 1024);
        check("跨段读取能读到边界前的消息", !across.isEmpty() && Arrays.equals(across.get(0), message((int) (boundary - 1))),
                "first=" + (across.isEmpty() ? "empty" : describe(across.get(0))));
        check("跨段读取能读到边界后的消息", across.size() >= 2 && Arrays.equals(across.get(1), message((int) boundary)),
                "size=" + across.size() + " second=" + (across.size() >= 2 ? describe(across.get(1)) : "n/a"));

        // 从段中间随机位置读（走稀疏索引 + 顺序扫描）
        long probe = boundary + 5;
        List<byte[]> mid = p.readMessages(probe, 64 * 1024);
        check("段中部随机读取正确", !mid.isEmpty() && Arrays.equals(mid.get(0), message((int) probe)),
                "probe=" + probe + " got=" + (mid.isEmpty() ? "empty" : describe(mid.get(0))));

        // 已越界 offset 读空
        check("offset 超过 LEO 返回空", p.readMessages(count, 64 * 1024).isEmpty(), "not empty");
        check("offset 远超过 LEO 返回空", p.readMessages(count + 1000, 64 * 1024).isEmpty(), "not empty");

        // ④ 稀疏索引体积：应远小于日志体积
        long logTotal = 0;
        long indexTotal = 0;
        for (File log : logs) {
            logTotal += log.length();
        }
        for (File index : indexes) {
            indexTotal += index.length();
        }
        double ratio = logTotal == 0 ? 0 : (indexTotal * 100.0 / logTotal);
        System.out.printf("  log=%d 字节, index=%d 字节, 索引占比=%.3f%%%n", logTotal, indexTotal, ratio);
        check("稀疏索引远小于日志数据（<1%%）", ratio < 1.0, "ratio=" + ratio + "%");

        p.close();
    }

    // ① 脏尾：长度头完整但消息体被截断
    private static void testDirtyTailIncompletePayload(File dir) throws Exception {
        section("① 脏尾恢复：消息体被截断");

        Partition p = new Partition(0, 1, new ArrayList<>(), dir.getAbsolutePath());
        for (int i = 0; i < 5; i++) {
            p.append(message(i));
        }
        long leo = p.getLogEndOffset();
        p.close();

        File log = filesWithSuffix(dir, ".log").get(0);
        long validLength = log.length();
        try (RandomAccessFile raf = new RandomAccessFile(log, "rw")) {
            raf.seek(raf.length());
            raf.writeInt(500);              // 声称有 500 字节
            raf.write(new byte[10]);        // 实际只有 10 字节
        }
        System.out.println("  污染前长度 = " + validLength + "，污染后长度 = " + log.length());

        Partition recovered = new Partition(0, 1, new ArrayList<>(), dir.getAbsolutePath());
        check("恢复后 LEO 不倒退（=5）", recovered.getLogEndOffset() == leo, "LEO=" + recovered.getLogEndOffset());
        check("脏尾被截断回原始长度", log.length() == validLength,
                "length=" + log.length() + " expected=" + validLength);
        List<byte[]> msgs = recovered.readMessages(0, 1024 * 1024);
        check("恢复后仍能读到 5 条消息", msgs.size() == 5, "size=" + msgs.size());
        for (int i = 0; i < Math.min(5, msgs.size()); i++) {
            check("恢复后 offset " + i + " 内容正确", Arrays.equals(msgs.get(i), message(i)), describe(msgs.get(i)));
        }
        // 恢复后继续追加，offset 衔接
        long next = recovered.append(message(5));
        check("恢复后追加 offset 衔接（=5）", next == 5, "offset=" + next);
        recovered.close();
    }

    // ① 脏尾：连 4 字节长度头都只写了一半
    private static void testDirtyTailHalfHeader(File dir) throws Exception {
        section("① 脏尾恢复：长度头只写了一半");

        Partition p = new Partition(0, 1, new ArrayList<>(), dir.getAbsolutePath());
        for (int i = 0; i < 3; i++) {
            p.append(message(i));
        }
        p.close();

        File log = filesWithSuffix(dir, ".log").get(0);
        long validLength = log.length();
        try (RandomAccessFile raf = new RandomAccessFile(log, "rw")) {
            raf.seek(raf.length());
            raf.write(new byte[] {0x00, 0x00}); // 半截长度头
        }

        Partition recovered = new Partition(0, 1, new ArrayList<>(), dir.getAbsolutePath());
        check("半截长度头：LEO 仍为 3", recovered.getLogEndOffset() == 3, "LEO=" + recovered.getLogEndOffset());
        check("半截长度头：已截断", log.length() == validLength, "length=" + log.length());
        check("半截长度头：仍能读到 3 条", recovered.readMessages(0, 1024 * 1024).size() == 3,
                "size=" + recovered.readMessages(0, 1024 * 1024).size());
        recovered.close();
    }

    // ① 脏尾：长度头本身是非法值（全 0xFF）
    private static void testGarbageLengthHeader(File dir) throws Exception {
        section("① 脏尾恢复：长度头是垃圾值");

        Partition p = new Partition(0, 1, new ArrayList<>(), dir.getAbsolutePath());
        for (int i = 0; i < 3; i++) {
            p.append(message(i));
        }
        p.close();

        File log = filesWithSuffix(dir, ".log").get(0);
        long validLength = log.length();
        try (RandomAccessFile raf = new RandomAccessFile(log, "rw")) {
            raf.seek(raf.length());
            raf.writeInt(0x7FFFFFFF); // 非法巨大长度
            raf.write(new byte[64]);
        }

        Partition recovered = new Partition(0, 1, new ArrayList<>(), dir.getAbsolutePath());
        check("垃圾长度头：LEO 仍为 3", recovered.getLogEndOffset() == 3, "LEO=" + recovered.getLogEndOffset());
        check("垃圾长度头：已截断", log.length() == validLength, "length=" + log.length());
        check("垃圾长度头：仍能读到 3 条", recovered.readMessages(0, 1024 * 1024).size() == 3,
                "size=" + recovered.readMessages(0, 1024 * 1024).size());
        recovered.close();
    }

    // ② 索引重建：索引被删/损坏也能自愈；复制路径幂等
    private static void testReplicationIdempotency(File dir) throws Exception {
        section("② 索引重建 / 复制幂等");

        Partition p = new Partition(0, 1, new ArrayList<>(), dir.getAbsolutePath());
        p.appendAtOffset(0, message(0));
        p.appendAtOffset(1, message(1));
        p.appendAtOffset(0, message(0)); // 重复投递，应被忽略
        p.appendAtOffset(1, message(1)); // 重复投递，应被忽略
        check("重复复制后 LEO = 2", p.getLogEndOffset() == 2, "LEO=" + p.getLogEndOffset());
        check("重复复制后仍只有 2 条", p.readMessages(0, 1024 * 1024).size() == 2,
                "size=" + p.readMessages(0, 1024 * 1024).size());
        for (int i = 2; i < 40; i++) {
            p.appendAtOffset(i, message(i));
        }
        long leo = p.getLogEndOffset();
        p.close();

        // 删除索引文件模拟索引损坏/丢失
        for (File index : filesWithSuffix(dir, ".index")) {
            check("删除索引文件 " + index.getName(), index.delete(), "delete failed");
        }
        Partition rebuilt = new Partition(0, 1, new ArrayList<>(), dir.getAbsolutePath());
        check("索引丢失后仍能读到 40 条", rebuilt.readMessages(0, 1024 * 1024).size() == 40,
                "size=" + rebuilt.readMessages(0, 1024 * 1024).size());
        List<File> indexes = filesWithSuffix(dir, ".index");
        check("索引文件被重建出来", !indexes.isEmpty() && indexes.get(0).length() > 0,
                "index files=" + indexes.size());
        check("重建后 LEO 不变", rebuilt.getLogEndOffset() == leo, "LEO=" + rebuilt.getLogEndOffset());
        rebuilt.close();
    }

    // ------------------------------------------------------------------
    // 工具
    // ------------------------------------------------------------------
    private static byte[] message(int index) {
        byte[] data = new byte[10_000];
        data[0] = (byte) (index >>> 24);
        data[1] = (byte) (index >>> 16);
        data[2] = (byte) (index >>> 8);
        data[3] = (byte) index;
        for (int i = 4; i < data.length; i++) {
            data[i] = (byte) (index & 0xFF);
        }
        return data;
    }

    private static String describe(byte[] data) {
        if (data == null) {
            return "null";
        }
        if (data.length < 4) {
            return "len=" + data.length;
        }
        int index = ((data[0] & 0xFF) << 24) | ((data[1] & 0xFF) << 16) | ((data[2] & 0xFF) << 8) | (data[3] & 0xFF);
        return "len=" + data.length + " index=" + index;
    }

    private static List<File> filesWithSuffix(File dir, String suffix) {
        List<File> result = new ArrayList<>();
        File[] files = dir.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.getName().endsWith(suffix)) {
                    result.add(file);
                }
            }
        }
        result.sort((a, b) -> a.getName().compareTo(b.getName()));
        return result;
    }

    private static List<String> names(List<File> files) {
        List<String> names = new ArrayList<>();
        for (File file : files) {
            names.add(file.getName());
        }
        return names;
    }

    private static void deleteRecursively(File file) {
        if (file.isDirectory()) {
            File[] children = file.listFiles();
            if (children != null) {
                for (File child : children) {
                    deleteRecursively(child);
                }
            }
        }
        file.delete();
    }

    private static void section(String title) {
        System.out.println();
        System.out.println("---- " + title + " ----");
    }

    private static void check(String what, boolean ok, Object detail) {
        if (ok) {
            System.out.println("  [PASS] " + what);
        } else {
            failures++;
            System.out.println("  [FAIL] " + what + "  (" + detail + ")");
        }
    }
}
