package com.simplekafka.broker;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Defines the wire protocol for Build Your Own Kafka
 */
public class Protocol {
    // Client request types
    public static final byte PRODUCE = 0x01;
    public static final byte FETCH = 0x02;
    public static final byte METADATA = 0x03;
    public static final byte CREATE_TOPIC = 0x04;

    // Broker response types
    public static final byte PRODUCE_RESPONSE = 0x11;
    public static final byte FETCH_RESPONSE = 0x12;
    public static final byte METADATA_RESPONSE = 0x13;
    public static final byte CREATE_TOPIC_RESPONSE = 0x14;
    public static final byte ERROR_RESPONSE = 0x1F;

    // Broker 间通信
    public static final byte TOPIC_NOTIFICATION = 0x23;
    /**
     * 副本拉取（follower → leader）：follower 主动按自己的 LEO 拉数据。
     *
     * <p>相比原来的 push 复制（leader 每写一条就推给 follower），pull 有两个关键好处：
     * ① follower 重启或落后后能自动追上来；② 每个 leader 只用一条连接批量拉，连接数可控。
     */
    public static final byte REPLICA_FETCH = 0x26;
    /** 副本拉取响应：type(1) + logStart(8) + leo(8) + count(4) + 每条[offset(8)+len(4)+payload]。 */
    public static final byte REPLICA_FETCH_RESPONSE = 0x27;

    /**
     * Send an error response to the client
     */
    public static void sendErrorResponse(SocketChannel channel, String errorMessage) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(3 + errorMessage.length());
        buffer.put(ERROR_RESPONSE);
        buffer.putShort((short) errorMessage.length());
        buffer.put(errorMessage.getBytes());
        buffer.flip();
        channel.write(buffer);
    }

    /**
     * Encode a producer request
     */
    public static ByteBuffer encodeProduceRequest(String topic, int partition, byte[] message) {
        ByteBuffer buffer = ByteBuffer.allocate(11 + topic.length() + message.length);
        buffer.put(PRODUCE);
        buffer.putShort((short) topic.length());
        buffer.put(topic.getBytes());
        buffer.putInt(partition);
        buffer.putInt(message.length);
        buffer.put(message);
        buffer.flip();
        return buffer;
    }

    /**
     * Encode a fetch request
     */
    public static ByteBuffer encodeFetchRequest(String topic, int partition, long offset, int maxBytes) {
        ByteBuffer buffer = ByteBuffer.allocate(19 + topic.length());
        buffer.put(FETCH);
        buffer.putShort((short) topic.length());
        buffer.put(topic.getBytes());
        buffer.putInt(partition);
        buffer.putLong(offset);
        buffer.putInt(maxBytes);
        buffer.flip();
        return buffer;
    }

    /**
     * Encode a metadata request
     */
    public static ByteBuffer encodeMetadataRequest() {
        ByteBuffer buffer = ByteBuffer.allocate(1);
        buffer.put(METADATA);
        buffer.flip();
        return buffer;
    }

    /**
     * Encode a create topic request
     */
    public static ByteBuffer encodeCreateTopicRequest(String topic, int numPartitions, short replicationFactor) {
        ByteBuffer buffer = ByteBuffer.allocate(9 + topic.length());
        buffer.put(CREATE_TOPIC);
        buffer.putShort((short) topic.length());
        buffer.put(topic.getBytes());
        buffer.putInt(numPartitions);
        buffer.putShort(replicationFactor);
        buffer.flip();
        return buffer;
    }

    /**
     * Encode a replica fetch request（副本拉取请求）。
     *
     * <p>字段长度：type(1) + topicLen(2) + topic(N) + partition(4) + offset(8) + maxBytes(4) + replicaId(4)
     */
    public static ByteBuffer encodeReplicaFetchRequest(String topic, int partition, long offset,
            int maxBytes, int replicaId) {
        ByteBuffer buffer = ByteBuffer.allocate(23 + topic.length());
        buffer.put(REPLICA_FETCH);
        buffer.putShort((short) topic.length());
        buffer.put(topic.getBytes());
        buffer.putInt(partition);
        buffer.putLong(offset);
        buffer.putInt(maxBytes);
        buffer.putInt(replicaId);
        buffer.flip();
        return buffer;
    }

    /**
     * Encode a replica fetch response（副本拉取响应，leader 侧使用）。
     *
     * @param logStartOffset leader 当前可读的最早 offset（follower 靠它发现自己落后太多）
     * @param logEndOffset   leader 当前的 LEO
     * @param startOffset    返回记录里第一条的 offset（后续依次 +1）
     */
    public static ByteBuffer encodeReplicaFetchResponse(long logStartOffset, long logEndOffset,
            long startOffset, List<byte[]> records) {
        int size = 1 + Long.BYTES * 2 + Integer.BYTES;
        for (byte[] record : records) {
            size += Long.BYTES + Integer.BYTES + record.length;
        }
        ByteBuffer buffer = ByteBuffer.allocate(size);
        buffer.put(REPLICA_FETCH_RESPONSE);
        buffer.putLong(logStartOffset);
        buffer.putLong(logEndOffset);
        buffer.putInt(records.size());
        long offset = startOffset;
        for (byte[] record : records) {
            buffer.putLong(offset++);
            buffer.putInt(record.length);
            buffer.put(record);
        }
        buffer.flip();
        return buffer;
    }

    /**
     * 从 leader 读一次副本拉取响应。
     *
     * <p>响应是自描述长度的（先类型，再 logStart/LEO/条数，然后逐条给长度），
     * 所以按长度精确读取即可，不受固定缓冲区大小限制（拉 1MB 也不会截断）。
     *
     * <p>用 {@link InputStream} 而不是 {@code SocketChannel}：前者配合普通 Socket 能真正
     * 遵守 {@code SO_TIMEOUT}，否则 leader 不响应时拉取线程会永久阻塞。
     */
    public static ReplicaFetchResult readReplicaFetchResponse(InputStream in) throws IOException {
        byte responseType = readFully(in, 1).get();

        if (responseType == ERROR_RESPONSE) {
            short length = readFully(in, Short.BYTES).getShort();
            String error = new String(readFully(in, length).array(), StandardCharsets.UTF_8);
            return new ReplicaFetchResult(-1L, -1L, new byte[0][], error);
        }
        if (responseType != REPLICA_FETCH_RESPONSE) {
            return new ReplicaFetchResult(-1L, -1L, new byte[0][], "unexpected response type: " + responseType);
        }

        ByteBuffer header = readFully(in, Long.BYTES * 2 + Integer.BYTES);
        long logStartOffset = header.getLong();
        long logEndOffset = header.getLong();
        int count = header.getInt();
        if (count < 0) {
            throw new IOException("invalid replica fetch record count: " + count);
        }

        byte[][] records = new byte[count][];
        for (int i = 0; i < count; i++) {
            ByteBuffer metadata = readFully(in, Long.BYTES + Integer.BYTES);
            metadata.getLong(); // offset 由调用方根据起始 offset 推算
            int size = metadata.getInt();
            if (size < 0) {
                throw new IOException("invalid record size: " + size);
            }
            records[i] = readFully(in, size).array();
        }
        return new ReplicaFetchResult(logStartOffset, logEndOffset, records, null);
    }

    /** 从流里精确读满 size 字节；对端提前关闭就报错而不是静默截断。 */
    private static ByteBuffer readFully(InputStream in, int size) throws IOException {
        if (size < 0) {
            throw new IOException("Invalid read size: " + size);
        }
        byte[] data = new byte[size];
        int total = 0;
        while (total < size) {
            int read = in.read(data, total, size - total);
            if (read < 0) {
                throw new IOException("Connection closed while reading replica fetch response");
            }
            total += read;
        }
        return ByteBuffer.wrap(data);
    }

    /** 副本拉取结果：leader 的起点/LEO + 一批记录。 */
    public static final class ReplicaFetchResult {
        private final long logStartOffset;
        private final long logEndOffset;
        private final byte[][] records;
        private final String error;

        private ReplicaFetchResult(long logStartOffset, long logEndOffset, byte[][] records, String error) {
            this.logStartOffset = logStartOffset;
            this.logEndOffset = logEndOffset;
            this.records = records;
            this.error = error;
        }

        public boolean isSuccess() {
            return error == null;
        }

        public long getLogStartOffset() {
            return logStartOffset;
        }

        public long getLogEndOffset() {
            return logEndOffset;
        }

        public byte[][] getRecords() {
            return records;
        }

        public String getError() {
            return error;
        }
    }

    /**
     * Encode a topic notification
     */
    public static ByteBuffer encodeTopicNotification(String topic) {
        ByteBuffer buffer = ByteBuffer.allocate(3 + topic.length());
        buffer.put(TOPIC_NOTIFICATION);
        buffer.putShort((short) topic.length());
        buffer.put(topic.getBytes());
        buffer.flip();
        return buffer;
    }

    /**
     * Decode a produce response
     */
    public static ProduceResult decodeProduceResponse(ByteBuffer buffer) {
        byte responseType = buffer.get();
        if (responseType != PRODUCE_RESPONSE) {
            if (responseType == ERROR_RESPONSE) {
                short errorLength = buffer.getShort();
                byte[] errorBytes = new byte[errorLength];
                buffer.get(errorBytes);
                String error = new String(errorBytes);
                return new ProduceResult(-1, error);
            }
            return new ProduceResult(-1, "Invalid response type");
        }

        long offset = buffer.getLong();
        byte status = buffer.get();

        return new ProduceResult(offset, status == 0 ? null : "Produce failed");
    }

    /**
     * Decode a fetch response
     */
    public static FetchResult decodeFetchResponse(ByteBuffer buffer) {
        byte responseType = buffer.get();
        if (responseType != FETCH_RESPONSE) {
            if (responseType == ERROR_RESPONSE) {
                short errorLength = buffer.getShort();
                byte[] errorBytes = new byte[errorLength];
                buffer.get(errorBytes);
                String error = new String(errorBytes);
                return new FetchResult(new byte[0][], error);
            }
            return new FetchResult(new byte[0][], "Invalid response type");
        }

        int messageCount = buffer.getInt();
        byte[][] messages = new byte[messageCount][];

        for (int i = 0; i < messageCount; i++) {
            long offset = buffer.getLong(); // Skip offset
            int messageSize = buffer.getInt();
            messages[i] = new byte[messageSize];
            buffer.get(messages[i]);
        }

        return new FetchResult(messages, null);
    }

    /**
     * Decode metadata response
     */
    public static MetadataResult decodeMetadataResponse(ByteBuffer buffer) {
        byte responseType = buffer.get();
        if (responseType != METADATA_RESPONSE) {
            if (responseType == ERROR_RESPONSE) {
                short errorLength = buffer.getShort();
                byte[] errorBytes = new byte[errorLength];
                buffer.get(errorBytes);
                String error = new String(errorBytes);
                return new MetadataResult(new ArrayList<>(), new ArrayList<>(), error);
            }
            return new MetadataResult(new ArrayList<>(), new ArrayList<>(), "Invalid response type");
        }

        // Parse broker info
        int brokerCount = buffer.getInt();
        List<BrokerInfo> brokers = new ArrayList<>();

        for (int i = 0; i < brokerCount; i++) {
            int brokerId = buffer.getInt();
            short hostLength = buffer.getShort();
            byte[] hostBytes = new byte[hostLength];
            buffer.get(hostBytes);
            String host = new String(hostBytes);
            int port = buffer.getInt();

            brokers.add(new BrokerInfo(brokerId, host, port));
        }

        // Parse topic metadata
        int topicCount = buffer.getInt();
        List<TopicMetadata> topics = new ArrayList<>();

        for (int i = 0; i < topicCount; i++) {
            short topicLength = buffer.getShort();
            byte[] topicBytes = new byte[topicLength];
            buffer.get(topicBytes);
            String topicName = new String(topicBytes);

            int partitionCount = buffer.getInt();
            List<PartitionMetadata> partitions = new ArrayList<>();

            for (int j = 0; j < partitionCount; j++) {
                int partitionId = buffer.getInt();
                int leaderId = buffer.getInt();

                int replicas = buffer.getInt();
                List<Integer> replicaIds = new ArrayList<>();

                for (int k = 0; k < replicas; k++) {
                    replicaIds.add(buffer.getInt());
                }

                partitions.add(new PartitionMetadata(partitionId, leaderId, replicaIds));
            }

            topics.add(new TopicMetadata(topicName, partitions));
        }

        return new MetadataResult(brokers, topics, null);
    }

    /**
     * Result class for produce operations
     */
    public static class ProduceResult {
        private final long offset;
        private final String error;

        public ProduceResult(long offset, String error) {
            this.offset = offset;
            this.error = error;
        }

        public long getOffset() {
            return offset;
        }

        public String getError() {
            return error;
        }

        public boolean isSuccess() {
            return error == null;
        }
    }

    /**
     * Result class for fetch operations
     */
    public static class FetchResult {
        private final byte[][] messages;
        private final String error;

        public FetchResult(byte[][] messages, String error) {
            this.messages = messages;
            this.error = error;
        }

        public byte[][] getMessages() {
            return messages;
        }

        public int getMessageCount() {
            return messages.length;
        }

        public String getError() {
            return error;
        }

        public boolean isSuccess() {
            return error == null;
        }
    }

    /**
     * Result class for metadata operations
     */
    public static class MetadataResult {
        private final List<BrokerInfo> brokers;
        private final List<TopicMetadata> topics;
        private final String error;

        public MetadataResult(List<BrokerInfo> brokers, List<TopicMetadata> topics, String error) {
            this.brokers = brokers;
            this.topics = topics;
            this.error = error;
        }

        public List<BrokerInfo> getBrokers() {
            return brokers;
        }

        public List<TopicMetadata> getTopics() {
            return topics;
        }

        public String getError() {
            return error;
        }

        public boolean isSuccess() {
            return error == null;
        }
    }

    /**
     * Topic metadata class
     */
    public static class TopicMetadata {
        private final String name;
        private final List<PartitionMetadata> partitions;

        public TopicMetadata(String name, List<PartitionMetadata> partitions) {
            this.name = name;
            this.partitions = partitions;
        }

        public String getName() {
            return name;
        }

        public List<PartitionMetadata> getPartitions() {
            return partitions;
        }
    }

    /**
     * Partition metadata class
     */
    public static class PartitionMetadata {
        private final int id;
        private final int leader;
        private final List<Integer> replicas;

        public PartitionMetadata(int id, int leader, List<Integer> replicas) {
            this.id = id;
            this.leader = leader;
            this.replicas = replicas;
        }

        public int getId() {
            return id;
        }

        public int getLeader() {
            return leader;
        }

        public List<Integer> getReplicas() {
            return replicas;
        }
    }
}
