package com.simplekafka.broker;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * 一个分区的本地存储层：一组按 offset 递增的 log segment + 稀疏索引。
 *
 * <p>目录布局（段文件名 = 该段第一条消息的 offset，与真实 Kafka 思路一致）：
 * <pre>
 *   data/&lt;brokerId&gt;/&lt;topic&gt;/&lt;partition&gt;/
 *     00000000000000000000.log     消息数据：每条记录 = [4B 长度][消息体]
 *     00000000000000000000.index   稀疏索引：每 ~4KB 数据一条，条目 = [4B 相对offset][4B 相对position]
 * </pre>
 *
 * <p>第一期持久化改造内容：
 * <ol>
 *   <li>① 崩溃恢复：启动扫描每个段，遇到不完整/非法记录即截断脏尾，保证 offset 不倒退、后续不被污染；</li>
 *   <li>③ 跨段读取：按段循环读取，跨越段边界能连续读下去（旧实现会在边界静默中断并泄漏句柄）；</li>
 *   <li>② 索引重建：索引被当作“可再生的加速结构”，启动时依据日志重建；</li>
 *   <li>④ 稀疏索引：条目由“每条消息 16B”降为“每 4KB 一条 8B”，读取时先二分定位再顺序扫描；</li>
 *   <li>⑤ 段轮转与索引归属：写入前判断本段是否放得下（段不超上限），索引始终写当前活跃段。</li>
 * </ol>
 */
public class Partition {
    private static final Logger LOGGER = Logger.getLogger(Partition.class.getName());

    /** 单段最大字节数（可用 -Dsimplekafka.segment.bytes 覆盖）。真实 Kafka 默认 1GB，这里 1MB 便于观察多段行为。 */
    public static final long MAX_SEGMENT_BYTES = Long.getLong("simplekafka.segment.bytes", 1024 * 1024L);
    /** 单个分区保留上限（字节，-Dsimplekafka.retention.bytes 可覆盖）；≤ 0 表示不限制。 */
    public static final long RETENTION_BYTES = Long.getLong("simplekafka.retention.bytes", 8 * 1024 * 1024L);
    /** 段保留时长（毫秒，-Dsimplekafka.retention.ms 可覆盖）；≤ 0 表示不限制。 */
    public static final long RETENTION_MS = Long.getLong("simplekafka.retention.ms", 3600_000L);
    /** 稀疏索引条目大小：[4B 相对 offset][4B 相对 position]。 */
    private static final int INDEX_ENTRY_BYTES = 8;
    /** 索引稀疏间隔（字节），对齐 Kafka 的 log.index.interval.bytes 默认值。 */
    private static final int INDEX_INTERVAL_BYTES = 4096;
    /** 单条记录长度上限，用于识别脏数据（防御性检查）。 */
    private static final int MAX_RECORD_BYTES = 16 * 1024 * 1024;

    private static final String LOG_SUFFIX = ".log";
    private static final String INDEX_SUFFIX = ".index";

    private final int id;
    private int leader;
    private List<Integer> followers;
    private final String baseDir;
    private final AtomicLong nextOffset;
    /** 分区里最早一条可用消息的 offset（保留策略删掉旧段后会前移）。 */
    private volatile long logStartOffset;
    private final ReadWriteLock lock;
    private final List<SegmentInfo> segments;

    private RandomAccessFile activeLogFile;
    private FileChannel activeLogChannel;
    private SegmentInfo activeSegment;

    public Partition(int id, int leader, List<Integer> followers, String baseDir) {
        this.id = id;
        this.leader = leader;
        this.followers = followers == null ? new ArrayList<>() : new ArrayList<>(followers);
        this.baseDir = baseDir;
        this.nextOffset = new AtomicLong(0);
        this.lock = new ReentrantReadWriteLock();
        this.segments = new ArrayList<>();

        initialize();
    }

    // ==================================================================
    // ① 崩溃恢复 + ② 索引重建
    // ==================================================================

    /**
     * 启动恢复：扫描目录里所有段，逐个校验并重建索引，最后打开最后一个段用于追加。
     */
    private void initialize() {
        File dir = new File(baseDir);
        if (!dir.exists() && !dir.mkdirs()) {
            throw new IllegalStateException("Failed to create partition directory: " + baseDir);
        }

        List<File> logFiles = new ArrayList<>();
        File[] files = dir.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.getName().endsWith(LOG_SUFFIX)) {
                    logFiles.add(file);
                }
            }
        }
        logFiles.sort(Comparator.comparing(File::getName));

        for (int i = 0; i < logFiles.size(); i++) {
            File logFile = logFiles.get(i);
            String baseName = baseName(logFile.getName());
            long baseOffset;
            try {
                baseOffset = Long.parseLong(baseName);
            } catch (NumberFormatException e) {
                LOGGER.warning("Ignoring unexpected file " + logFile.getName() + " in " + baseDir);
                continue;
            }

            SegmentInfo segment = new SegmentInfo(baseOffset, logFile, new File(dir, baseName + INDEX_SUFFIX));
            long count = recoverSegment(segment);
            segments.add(segment);
            if (i == logFiles.size() - 1) {
                nextOffset.set(baseOffset + count);
            }
        }

        try {
            if (segments.isEmpty()) {
                createNewSegment(0L);
                nextOffset.set(0L);
                logStartOffset = 0L;
            } else {
                openSegmentForAppend(segments.get(segments.size() - 1));
                logStartOffset = segments.get(0).baseOffset;
            }
        } catch (IOException e) {
            throw new IllegalStateException("Failed to open active segment for partition " + id, e);
        }

        LOGGER.info("Initialized partition " + id + " with " + segments.size()
                + " segments, log start: " + logStartOffset + ", next offset: " + nextOffset.get());
    }

    /**
     * 扫描单个段：校验每条记录、截断脏尾、按稀疏规则重建索引。
     *
     * @return 段内有效消息条数（等于下一个相对 offset）
     */
    private long recoverSegment(SegmentInfo segment) {
        long messageCount = 0L;
        long position = 0L;
        long lastIndexPosition = -1L;
        List<long[]> indexEntries = new ArrayList<>();

        try (RandomAccessFile logFile = new RandomAccessFile(segment.logFile, "rw");
             FileChannel channel = logFile.getChannel()) {

            long fileSize = channel.size();
            ByteBuffer header = ByteBuffer.allocate(Integer.BYTES);

            while (position + Integer.BYTES <= fileSize) {
                header.clear();
                if (!readFully(channel, header, position)) {
                    break;
                }
                header.flip();
                int length = header.getInt();

                // 长度非法或数据不足 → 从这里开始是崩溃残留的脏尾
                if (length <= 0 || length > MAX_RECORD_BYTES || position + Integer.BYTES + length > fileSize) {
                    LOGGER.warning("Dirty tail detected in " + segment.logFile.getName()
                            + " at position " + position + " (record length=" + length + ")");
                    break;
                }

                if (lastIndexPosition < 0 || position - lastIndexPosition >= INDEX_INTERVAL_BYTES) {
                    indexEntries.add(new long[] {messageCount, position});
                    lastIndexPosition = position;
                }

                position += Integer.BYTES + length;
                messageCount++;
            }

            if (position < fileSize) {
                // 截断脏尾：保证后续追加不会落在半条记录之后
                logFile.setLength(position);
                channel.force(true);
                LOGGER.warning("Recovered " + segment.logFile.getName() + ": dropped "
                        + (fileSize - position) + " incomplete byte(s), kept " + messageCount + " message(s)");
            }
        } catch (IOException e) {
            throw new IllegalStateException("Failed to recover segment " + segment.logFile.getName(), e);
        }

        rebuildIndex(segment, indexEntries);
        segment.size = position;
        segment.messageCount = messageCount;
        segment.lastIndexPosition = indexEntries.isEmpty() ? -1L : indexEntries.get(indexEntries.size() - 1)[1];
        return messageCount;
    }

    /** 依据扫描结果重建索引文件（索引可再生，格式不符或损坏都能自愈）。 */
    private void rebuildIndex(SegmentInfo segment, List<long[]> entries) {
        try (RandomAccessFile indexFile = new RandomAccessFile(segment.indexFile, "rw");
             FileChannel channel = indexFile.getChannel()) {
            channel.truncate(0L);
            ByteBuffer buffer = ByteBuffer.allocate(entries.size() * INDEX_ENTRY_BYTES);
            for (long[] entry : entries) {
                buffer.putInt((int) entry[0]).putInt((int) entry[1]);
            }
            buffer.flip();
            writeFully(channel, buffer, 0L);
            channel.force(true);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to rebuild index for " + segment.logFile.getName(), e);
        }
    }

    // ==================================================================
    // 写入：④ 稀疏索引、⑤ 段轮转与索引归属
    // ==================================================================

    /**
     * 追加一条消息（leader 路径）。
     *
     * @return 分配到的 offset
     */
    public long append(byte[] message) {
        requireMessage(message);
        lock.writeLock().lock();
        try {
            long offset = nextOffset.get();
            writeRecord(offset, message);
            nextOffset.set(offset + 1);
            return offset;
        } catch (IOException e) {
            throw new IllegalStateException("Failed to append message to partition " + id, e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * 按 leader 分配的 offset 写入（follower 复制路径），保证副本之间 offset 严格对齐。
     *
     * <p>重复 offset 幂等忽略；出现空洞（本地位点落后于 leader）时记录警告并按 leader 的 offset 继续。
     */
    public void appendAtOffset(long offset, byte[] message) {
        requireMessage(message);
        if (offset < 0) {
            throw new IllegalArgumentException("offset must not be negative");
        }

        lock.writeLock().lock();
        try {
            if (offset < nextOffset.get()) {
                return; // 已经复制过这条消息，幂等忽略
            }
            if (offset > nextOffset.get()) {
                LOGGER.warning("Replication gap on partition " + id + ": local end=" + nextOffset.get()
                        + ", leader offset=" + offset + ", jumping forward");
            }
            writeRecord(offset, message);
            nextOffset.set(offset + 1);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to append replicated message to partition " + id, e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * 写入一条记录（需要持有写锁）。
     *
     * <p>⑤ 轮转策略：写入前判断本段放不放得下这条消息，放不下且本段已有消息时滚动新段，
     * 因此段不会超过上限，也不会产生空段。
     */
    private void writeRecord(long offset, byte[] message) throws IOException {
        ensureActiveSegment();
        if (activeSegment.messageCount > 0
                && activeSegment.size + Integer.BYTES + message.length > MAX_SEGMENT_BYTES) {
            createNewSegment(offset);
        }

        long startPosition = activeSegment.size;
        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES + message.length);
        buffer.putInt(message.length);
        buffer.put(message);
        buffer.flip();
        writeFully(activeLogChannel, buffer, startPosition);
        activeLogChannel.force(true);

        appendIndexEntryIfNeeded(activeSegment, offset - activeSegment.baseOffset, startPosition);

        activeSegment.size = startPosition + Integer.BYTES + message.length;
        activeSegment.messageCount++;
    }

    /** ④ 稀疏索引：距上次索引项不足 INDEX_INTERVAL_BYTES 就跳过，不再为每条消息写索引。 */
    private void appendIndexEntryIfNeeded(SegmentInfo segment, long relativeOffset, long position) throws IOException {
        if (segment.lastIndexPosition >= 0
                && position - segment.lastIndexPosition < INDEX_INTERVAL_BYTES) {
            return;
        }
        try (RandomAccessFile indexFile = new RandomAccessFile(segment.indexFile, "rw");
             FileChannel channel = indexFile.getChannel()) {
            ByteBuffer entry = ByteBuffer.allocate(INDEX_ENTRY_BYTES);
            entry.putInt((int) relativeOffset).putInt((int) position);
            entry.flip();
            writeFully(channel, entry, channel.size());
            channel.force(true);
        }
        segment.lastIndexPosition = position;
    }

    // ==================================================================
    // 保留策略（log retention）：删除过旧的段
    // ==================================================================

    /**
     * 按保留策略删除过旧的段（对应 Kafka 的 log.retention.*）。
     *
     * <p>规则：
     * <ul>
     *   <li>绝不删除活跃段，也绝不让分区变成零个段；</li>
     *   <li>最旧的段“太旧”（文件最后修改时间超过 {@link #RETENTION_MS}）
     *       或“太大”（总字节数超过 {@link #RETENTION_BYTES}）时，从最旧的段开始删。</li>
     * </ul>
     *
     * <p>删除后 {@link #getLogStartOffset()} 会前移：消费者再去读已经被删掉的 offset 会得到
     * “offset 越界”错误，而不会拿到被错位标注的新数据。
     *
     * @return 本次删除的段数
     */
    public int cleanup() {
        lock.writeLock().lock();
        try {
            int deleted = 0;
            long total = totalSizeLocked();
            while (segments.size() > 1) {
                SegmentInfo oldest = segments.get(0);
                if (oldest == activeSegment) {
                    break;
                }
                boolean tooOld = RETENTION_MS > 0
                        && System.currentTimeMillis() - oldest.logFile.lastModified() > RETENTION_MS;
                boolean tooBig = RETENTION_BYTES > 0 && total > RETENTION_BYTES;
                if (!tooOld && !tooBig) {
                    break;
                }
                deleteSegment(oldest);
                total -= oldest.size;
                deleted++;
            }
            if (deleted > 0) {
                logStartOffset = segments.get(0).baseOffset;
                LOGGER.info("Retention deleted " + deleted + " segment(s) of partition " + id
                        + ", log start offset is now " + logStartOffset + ", remaining bytes " + total);
            }
            return deleted;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * 丢弃 offset 之前的全部数据，把分区起点对齐到该 offset。
     *
     * <p>专用于 follower 追赶：leader 按保留策略删掉了 follower 还没读到的老段时，
     * follower 的 LEO 会小于 leader 的 logStartOffset，唯一正确的做法是丢掉自己超出
     * 范围的老数据、把起点对齐到 leader（Kafka 里对应 follower 的日志截断）。
     *
     * <p>如果目标起点已经超过本分区 LEO，则整个分区被清空，并以该 offset 作为新起点。
     */
    public void truncateBefore(long newStartOffset) {
        lock.writeLock().lock();
        try {
            if (newStartOffset <= logStartOffset) {
                return;
            }
            if (newStartOffset >= nextOffset.get()) {
                // 完全落后于 leader 的保留起点：清空并重定起点
                closeActiveChannel();
                for (SegmentInfo segment : new ArrayList<>(segments)) {
                    deleteSegment(segment);
                }
                segments.clear();
                activeSegment = null;
                createNewSegment(newStartOffset);
                nextOffset.set(newStartOffset);
                logStartOffset = newStartOffset;
                LOGGER.warning("Partition " + id + " was entirely behind leader retention; reset to offset "
                        + newStartOffset);
                return;
            }

            int deleted = 0;
            while (segments.size() > 1) {
                SegmentInfo oldest = segments.get(0);
                if (oldest == activeSegment) {
                    break;
                }
                if (segments.get(1).baseOffset > newStartOffset) {
                    break; // 该段里已经包含 >= newStartOffset 的数据，不能再删
                }
                deleteSegment(oldest);
                deleted++;
            }
            long previous = logStartOffset;
            logStartOffset = segments.get(0).baseOffset;
            if (deleted > 0) {
                LOGGER.warning("Truncated partition " + id + ": log start " + previous + " → " + logStartOffset
                        + " to catch up with leader");
            }
        } catch (IOException e) {
            throw new IllegalStateException("Failed to truncate partition " + id + " to " + newStartOffset, e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /** 删除一个段（日志文件 + 索引文件）。调用方必须持有写锁。 */
    private void deleteSegment(SegmentInfo segment) {
        segments.remove(segment);
        if (!segment.logFile.delete()) {
            LOGGER.warning("Failed to delete log segment " + segment.logFile.getAbsolutePath());
        }
        if (segment.indexFile.exists() && !segment.indexFile.delete()) {
            LOGGER.warning("Failed to delete index file " + segment.indexFile.getAbsolutePath());
        }
        LOGGER.info("Deleted segment " + segment.logFile.getName() + " of partition " + id
                + " (" + segment.size + " bytes)");
    }

    /** 日志总字节数（不含索引）。调用方必须持有读锁或写锁。 */
    private long totalSizeLocked() {
        long total = 0L;
        for (SegmentInfo segment : segments) {
            total += segment.size;
        }
        return total;
    }

    private void ensureActiveSegment() throws IOException {
        if (activeSegment != null && activeLogChannel != null) {
            return;
        }
        if (segments.isEmpty()) {
            createNewSegment(nextOffset.get());
        } else {
            openSegmentForAppend(segments.get(segments.size() - 1));
        }
    }

    /** 新建一个段并切换为活跃段（段文件名 = baseOffset）。 */
    private SegmentInfo createNewSegment(long baseOffset) throws IOException {
        String baseName = formatBaseOffset(baseOffset);
        File logFile = new File(baseDir, baseName + LOG_SUFFIX);
        File indexFile = new File(baseDir, baseName + INDEX_SUFFIX);
        Files.write(logFile.toPath(), new byte[0], StandardOpenOption.CREATE,
                StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);
        Files.write(indexFile.toPath(), new byte[0], StandardOpenOption.CREATE,
                StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);

        SegmentInfo segment = new SegmentInfo(baseOffset, logFile, indexFile);
        segments.add(segment);
        openSegmentForAppend(segment);
        LOGGER.info("Created new segment " + logFile.getName() + " for partition " + id);
        return segment;
    }

    private void openSegmentForAppend(SegmentInfo segment) throws IOException {
        closeActiveChannel();
        activeLogFile = new RandomAccessFile(segment.logFile, "rw");
        activeLogChannel = activeLogFile.getChannel();
        activeLogChannel.position(segment.size);
        activeSegment = segment;
    }

    private void closeActiveChannel() {
        try {
            if (activeLogChannel != null) {
                activeLogChannel.close();
            }
        } catch (IOException e) {
            LOGGER.log(Level.WARNING, "Failed to close active log channel of partition " + id, e);
        } finally {
            activeLogChannel = null;
        }
        try {
            if (activeLogFile != null) {
                activeLogFile.close();
            }
        } catch (IOException e) {
            LOGGER.log(Level.WARNING, "Failed to close active log file of partition " + id, e);
        } finally {
            activeLogFile = null;
        }
    }

    // ==================================================================
    // ③ 跨段读取 + ④ 稀疏索引定位
    // ==================================================================

    /**
     * 从指定 offset 开始读取消息，最多读取 maxBytes 字节。
     *
     * <p>③ 按段循环：段内先用稀疏索引定位再顺序扫描，读到段末尾就切换到下一个段继续，
     * 因此跨段边界也能连续读取（旧实现会在边界处静默中断，并泄漏新开句柄）。
     */
    public List<byte[]> readMessages(long offset, int maxBytes) {
        if (offset < 0) {
            throw new IllegalArgumentException("offset must not be negative");
        }
        if (offset < logStartOffset) {
            // 老数据已被保留策略删除：绝不能把幸存的新数据错位标注成被请求的 offset
            throw new IllegalArgumentException("offset " + offset
                    + " is before log start offset " + logStartOffset + " (deleted by retention)");
        }
        if (maxBytes <= 0) {
            return Collections.emptyList();
        }

        lock.readLock().lock();
        try {
            List<byte[]> messages = new ArrayList<>();
            long logEnd = nextOffset.get();
            long currentOffset = offset;
            int remaining = maxBytes;

            while (remaining > 0 && currentOffset < logEnd) {
                int segmentIndex = findSegmentIndexForOffset(currentOffset);
                if (segmentIndex < 0) {
                    break;
                }
                SegmentInfo segment = segments.get(segmentIndex);
                long upperBound = segmentIndex + 1 < segments.size()
                        ? segments.get(segmentIndex + 1).baseOffset
                        : logEnd;

                long offsetAtPosition = currentOffset;
                long position = 0L;

                try (RandomAccessFile logFile = new RandomAccessFile(segment.logFile, "r");
                     FileChannel channel = logFile.getChannel()) {

                    // ④ 用稀疏索引把扫描起点拉到目标 offset 之前最近的位置
                    IndexEntry entry = findIndexEntry(segment, currentOffset);
                    offsetAtPosition = segment.baseOffset + entry.offset;
                    position = entry.position;

                    long fileSize = channel.size();
                    ByteBuffer header = ByteBuffer.allocate(Integer.BYTES);

                    while (remaining > 0 && offsetAtPosition < upperBound
                            && position + Integer.BYTES <= fileSize) {
                        header.clear();
                        if (!readFully(channel, header, position)) {
                            break;
                        }
                        header.flip();
                        int length = header.getInt();
                        if (length <= 0 || position + Integer.BYTES + length > fileSize) {
                            break;
                        }

                        if (offsetAtPosition < currentOffset) {
                            // 稀疏索引落在目标之前，顺序跳过中间消息
                            position += Integer.BYTES + length;
                            offsetAtPosition++;
                            continue;
                        }
                        if (Integer.BYTES + length > remaining) {
                            break;
                        }

                        ByteBuffer payload = ByteBuffer.allocate(length);
                        if (!readFully(channel, payload, position + Integer.BYTES)) {
                            break;
                        }
                        messages.add(payload.array());
                        remaining -= Integer.BYTES + length;
                        position += Integer.BYTES + length;
                        offsetAtPosition++;
                    }
                } catch (IOException e) {
                    throw new IllegalStateException("Failed to read messages from partition " + id, e);
                }

                if (offsetAtPosition == currentOffset) {
                    break; // 本段没有进展，避免死循环
                }
                currentOffset = offsetAtPosition;
            }
            return messages;
        } finally {
            lock.readLock().unlock();
        }
    }

    /** 二分查找 offset 所属的段下标（baseOffset ≤ offset 的最大者）。 */
    private int findSegmentIndexForOffset(long offset) {
        if (segments.isEmpty()) {
            return -1;
        }
        int low = 0;
        int high = segments.size() - 1;
        int candidate = 0;
        while (low <= high) {
            int mid = (low + high) >>> 1;
            if (segments.get(mid).baseOffset <= offset) {
                candidate = mid;
                low = mid + 1;
            } else {
                high = mid - 1;
            }
        }
        return candidate;
    }

    /**
     * 在稀疏索引里二分查找“最接近且不大于目标 offset”的条目。
     *
     * <p>返回的条目是扫描起点，调用方需从该位置顺序扫描到目标 offset。
     * 索引为空时返回 (0, 0)，即从段头开始扫描。
     */
    private IndexEntry findIndexEntry(SegmentInfo segment, long targetOffset) throws IOException {
        long relativeTarget = targetOffset - segment.baseOffset;
        if (!segment.indexFile.exists() || segment.indexFile.length() < INDEX_ENTRY_BYTES) {
            return new IndexEntry(0L, 0L);
        }

        try (RandomAccessFile indexFile = new RandomAccessFile(segment.indexFile, "r");
             FileChannel channel = indexFile.getChannel()) {

            long entryCount = channel.size() / INDEX_ENTRY_BYTES;
            ByteBuffer buffer = ByteBuffer.allocate(INDEX_ENTRY_BYTES);
            long low = 0;
            long high = entryCount - 1;
            long bestOffset = 0L;
            long bestPosition = 0L;

            while (low <= high) {
                long mid = (low + high) >>> 1;
                buffer.clear();
                if (!readFully(channel, buffer, mid * INDEX_ENTRY_BYTES)) {
                    break;
                }
                buffer.flip();
                long entryOffset = buffer.getInt() & 0xFFFFFFFFL;
                long entryPosition = buffer.getInt() & 0xFFFFFFFFL;
                if (entryOffset <= relativeTarget) {
                    bestOffset = entryOffset;
                    bestPosition = entryPosition;
                    low = mid + 1;
                } else {
                    high = mid - 1;
                }
            }
            return new IndexEntry(bestOffset, bestPosition);
        }
    }

    // ==================================================================
    // 元数据与生命周期
    // ==================================================================

    public int getId() {
        return id;
    }

    public int getLeader() {
        return leader;
    }

    public void setLeader(int leader) {
        this.leader = leader;
    }

    public List<Integer> getFollowers() {
        return new ArrayList<>(followers);
    }

    public void setFollowers(List<Integer> followers) {
        this.followers = new ArrayList<>(followers);
    }

    /** 下一条待写入消息的 offset（log end offset / LEO）。 */
    public long getLogEndOffset() {
        return nextOffset.get();
    }

    /** 仍然可读的最早 offset（log start offset）；小于它的 offset 已被保留策略删除。 */
    public long getLogStartOffset() {
        return logStartOffset;
    }

    /** 分区当前占用的日志字节数（不含索引）。 */
    public long getTotalSize() {
        lock.readLock().lock();
        try {
            return totalSizeLocked();
        } finally {
            lock.readLock().unlock();
        }
    }

    /** 当前段数量。 */
    public int getSegmentCount() {
        lock.readLock().lock();
        try {
            return segments.size();
        } finally {
            lock.readLock().unlock();
        }
    }

    /** 关闭活跃段释放句柄。 */
    public void close() {
        lock.writeLock().lock();
        try {
            closeActiveChannel();
            activeSegment = null;
        } finally {
            lock.writeLock().unlock();
        }
    }

    // ==================================================================
    // 基础工具
    // ==================================================================

    private static void requireMessage(byte[] message) {
        if (message == null) {
            throw new IllegalArgumentException("message must not be null");
        }
    }

    private static String baseName(String fileName) {
        return fileName.substring(0, fileName.length() - LOG_SUFFIX.length());
    }

    private static String formatBaseOffset(long baseOffset) {
        return String.format("%020d", baseOffset);
    }

    /** 从指定位置完整读满 buffer；没法读满返回 false（说明到了文件末尾）。 */
    private static boolean readFully(FileChannel channel, ByteBuffer buffer, long position) throws IOException {
        while (buffer.hasRemaining()) {
            int read = channel.read(buffer, position);
            if (read < 0) {
                return false;
            }
            position += read;
        }
        return true;
    }

    /** 从指定位置把 buffer 全部写入；无进展则报错，避免静默丢数据。 */
    private static void writeFully(FileChannel channel, ByteBuffer buffer, long position) throws IOException {
        while (buffer.hasRemaining()) {
            int written = channel.write(buffer, position);
            if (written <= 0) {
                throw new IOException("Failed to make progress while writing to disk");
            }
            position += written;
        }
    }

    /** 稀疏索引查询结果：相对 offset 与其在段内的物理位置。 */
    private static final class IndexEntry {
        private final long offset;
        private final long position;

        private IndexEntry(long offset, long position) {
            this.offset = offset;
            this.position = position;
        }
    }

    /** 段元数据：段名里的 base offset + 运行时状态（大小、消息数、最近索引位置）。 */
    private static final class SegmentInfo {
        private final long baseOffset;
        private final File logFile;
        private final File indexFile;
        private long size;
        private long messageCount;
        private long lastIndexPosition = -1L;

        private SegmentInfo(long baseOffset, File logFile, File indexFile) {
            this.baseOffset = baseOffset;
            this.logFile = logFile;
            this.indexFile = indexFile;
        }
    }
}
