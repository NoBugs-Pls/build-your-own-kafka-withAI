package com.simplekafka.client;

import com.simplekafka.broker.BrokerInfo;
import com.simplekafka.broker.Protocol;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Client for interacting with Build Your Own Kafka brokers
 */
public class SimpleKafkaClient {
    private static final Logger LOGGER = Logger.getLogger(SimpleKafkaClient.class.getName());
    // 元数据响应随 broker / topic / 分区数量线性增长，缓冲区留足余量以支持自由增删节点
    private static final int DEFAULT_BUFFER_SIZE = 64 * 1024;
    private static final int CONNECT_TIMEOUT_MS = 3000;
    private static final int READ_TIMEOUT_MS = 5000;

    private final String bootstrapBroker;
    private final int bootstrapPort;
    private final Map<String, TopicMetadata> topicMetadata;
    private final Map<Integer, BrokerInfo> brokers;
    private final AtomicInteger correlationId;

    /**
     * Create a SimpleKafka client
     * @param bootstrapBroker Host of a broker to connect to
     * @param bootstrapPort Port of the broker to connect to
     */
    public SimpleKafkaClient(String bootstrapBroker, int bootstrapPort) {
        this.bootstrapBroker = bootstrapBroker;
        this.bootstrapPort = bootstrapPort;
        this.topicMetadata = new ConcurrentHashMap<>();
        this.brokers = new ConcurrentHashMap<>();
        this.correlationId = new AtomicInteger(0);
    }

    /**
     * Connect a socket channel with a bounded timeout so a dead broker cannot
     * hang the caller forever.
     */
    private static SocketChannel connect(SocketChannel channel, InetSocketAddress address) throws IOException {
        Socket socket = channel.socket();
        socket.connect(address, CONNECT_TIMEOUT_MS);
        socket.setSoTimeout(READ_TIMEOUT_MS);
        return channel;
    }

    /**
     * Initialize the client by fetching cluster metadata
     */
    public void initialize() throws IOException {
        refreshMetadata();
    }

    /**
     * Refresh metadata about brokers and topics
     */
    public void refreshMetadata() throws IOException {
        try (SocketChannel channel = SocketChannel.open()) {
            connect(channel, new InetSocketAddress(bootstrapBroker, bootstrapPort));

            // Request metadata
            ByteBuffer request = Protocol.encodeMetadataRequest();
            channel.write(request);

            // Read response
            ByteBuffer response = ByteBuffer.allocate(DEFAULT_BUFFER_SIZE);
            int bytesRead = channel.read(response);
            if (bytesRead <= 0) {
                throw new IOException("No data received from broker");
            }

            response.flip();

            // Parse response
            Protocol.MetadataResult result = Protocol.decodeMetadataResponse(response);

            if (!result.isSuccess()) {
                throw new IOException("Failed to fetch metadata: " + result.getError());
            }

            // Update broker information
            brokers.clear();
            for (BrokerInfo broker : result.getBrokers()) {
                brokers.put(broker.getId(), new BrokerInfo(broker.getId(), broker.getHost(), broker.getPort()));
            }

            // Update topic metadata
            topicMetadata.clear();
            for (Protocol.TopicMetadata topic : result.getTopics()) {
                List<PartitionInfo> partitions = new ArrayList<>();

                for (Protocol.PartitionMetadata partition : topic.getPartitions()) {
                    partitions.add(new PartitionInfo(
                        partition.getId(),
                        partition.getLeader(),
                        partition.getReplicas()
                    ));
                }

                topicMetadata.put(topic.getName(), new TopicMetadata(topic.getName(), partitions));
            }

            LOGGER.info("Metadata refreshed: " + brokers.size() + " brokers, " +
                       topicMetadata.size() + " topics");
        }
    }

    /**
     * Create a new topic
     */
    public boolean createTopic(String topic, int numPartitions, short replicationFactor) throws IOException {
        // Find a broker to send the request to (prefer controller if known)
        if (brokers.isEmpty()) {
            refreshMetadata();
            if (brokers.isEmpty()) {
                throw new IOException("No brokers available");
            }
        }

        BrokerInfo broker = brokers.values().iterator().next();

        try (SocketChannel channel = SocketChannel.open()) {
            connect(channel, new InetSocketAddress(broker.getHost(), broker.getPort()));

            // Send create topic request
            ByteBuffer request = Protocol.encodeCreateTopicRequest(topic, numPartitions, replicationFactor);
            channel.write(request);

            // Read response
            ByteBuffer response = ByteBuffer.allocate(DEFAULT_BUFFER_SIZE);
            int bytesRead = channel.read(response);
            if (bytesRead <= 0) {
                throw new IOException("No data received from broker");
            }

            response.flip();

            byte responseType = response.get();
            if (responseType != Protocol.CREATE_TOPIC_RESPONSE) {
                if (responseType == Protocol.ERROR_RESPONSE) {
                    short errorLength = response.getShort();
                    byte[] errorBytes = new byte[errorLength];
                    response.get(errorBytes);
                    String error = new String(errorBytes);
                    LOGGER.warning("Error creating topic: " + error);
                    return false;
                }
                throw new IOException("Invalid create topic response type: " + responseType);
            }

            byte status = response.get();
            boolean success = status == 0;

            if (success) {
                // Refresh metadata to include new topic
                refreshMetadata();
            }

            return success;
        }
    }

    /**
     * Produce a message to a topic-partition
     */
    public long send(String topic, int partition, byte[] message) throws IOException {
        if (!topicMetadata.containsKey(topic)) {
            refreshMetadata();
            if (!topicMetadata.containsKey(topic)) {
                throw new IOException("Topic not found: " + topic);
            }
        }

        // Find the leader for the partition
        TopicMetadata metadata = topicMetadata.get(topic);
        PartitionInfo partitionInfo = null;

        for (PartitionInfo info : metadata.getPartitions()) {
            if (info.getId() == partition) {
                partitionInfo = info;
                break;
            }
        }

        if (partitionInfo == null) {
            throw new IOException("Partition not found: " + partition);
        }

        int leaderId = partitionInfo.getLeader();
        BrokerInfo leader = brokers.get(leaderId);

        if (leader == null) {
            refreshMetadata();
            leader = brokers.get(leaderId);

            if (leader == null) {
                throw new IOException("Leader broker not found: " + leaderId);
            }
        }

        // Send to leader
        try (SocketChannel channel = SocketChannel.open()) {
            connect(channel, new InetSocketAddress(leader.getHost(), leader.getPort()));

            // Send produce request
            ByteBuffer request = Protocol.encodeProduceRequest(topic, partition, message);
            channel.write(request);

            // Read response
            ByteBuffer response = ByteBuffer.allocate(DEFAULT_BUFFER_SIZE);
            int bytesRead = channel.read(response);
            if (bytesRead <= 0) {
                throw new IOException("No data received from broker");
            }

            response.flip();

            Protocol.ProduceResult result = Protocol.decodeProduceResponse(response);

            if (!result.isSuccess()) {
                throw new IOException("Failed to produce message: " + result.getError());
            }

            return result.getOffset();
        }
    }

    /**
     * Consume messages from a topic-partition
     */
    public List<byte[]> fetch(String topic, int partition, long offset, int maxBytes) throws IOException {
        if (!topicMetadata.containsKey(topic)) {
            refreshMetadata();
            if (!topicMetadata.containsKey(topic)) {
                throw new IOException("Topic not found: " + topic);
            }
        }

        // Find the leader for the partition
        TopicMetadata metadata = topicMetadata.get(topic);
        PartitionInfo partitionInfo = null;

        for (PartitionInfo info : metadata.getPartitions()) {
            if (info.getId() == partition) {
                partitionInfo = info;
                break;
            }
        }

        if (partitionInfo == null) {
            throw new IOException("Partition not found: " + partition);
        }

        int leaderId = partitionInfo.getLeader();
        BrokerInfo leader = brokers.get(leaderId);

        if (leader == null) {
            refreshMetadata();
            leader = brokers.get(leaderId);

            if (leader == null) {
                throw new IOException("Leader broker not found: " + leaderId);
            }
        }

        // Fetch from leader
        try (SocketChannel channel = SocketChannel.open()) {
            connect(channel, new InetSocketAddress(leader.getHost(), leader.getPort()));

            // Send fetch request
            ByteBuffer request = Protocol.encodeFetchRequest(topic, partition, offset, maxBytes);
            writeFully(channel, request);

            return readFetchResponse(channel);
        }
    }

    /**
     * 精确读取一次 fetch 响应。
     *
     * <p>fetch 响应是“自描述长度”的（1 字节类型 + 4 字节条数，随后逐条给出长度），
     * 因此不需要猜测响应总大小：拿 1MB 的大批量也能完整读完，
     * 不会再出现固定缓冲区装满后解码 BufferUnderflowException 的问题。
     */
    private static List<byte[]> readFetchResponse(SocketChannel channel) throws IOException {
        byte responseType = readFully(channel, 1).get();

        if (responseType == Protocol.ERROR_RESPONSE) {
            short length = readFully(channel, Short.BYTES).getShort();
            byte[] text = readFully(channel, length).array();
            throw new IOException("Failed to fetch messages: " + new String(text, StandardCharsets.UTF_8));
        }
        if (responseType != Protocol.FETCH_RESPONSE) {
            throw new IOException("Invalid response type: " + responseType);
        }

        int messageCount = readFully(channel, Integer.BYTES).getInt();
        if (messageCount < 0) {
            throw new IOException("Invalid message count: " + messageCount);
        }

        List<byte[]> messages = new ArrayList<>(Math.min(messageCount, 1024));
        for (int i = 0; i < messageCount; i++) {
            ByteBuffer metadata = readFully(channel, Long.BYTES + Integer.BYTES);
            metadata.getLong(); // offset 由调用方根据起始 offset 推算
            int size = metadata.getInt();
            if (size < 0) {
                throw new IOException("Invalid message size: " + size);
            }
            messages.add(readFully(channel, size).array());
        }
        return messages;
    }

    /** 把 buffer 全部写到 socket（单次 write 可能只写出一部分）。 */
    private static void writeFully(SocketChannel channel, ByteBuffer buffer) throws IOException {
        while (buffer.hasRemaining()) {
            int written = channel.write(buffer);
            if (written < 0) {
                throw new IOException("Connection closed by broker while writing request");
            }
        }
    }

    /** 从 socket 精确读满 size 字节，对端提前关闭就报错而不是静默截断。 */
    private static ByteBuffer readFully(SocketChannel channel, int size) throws IOException {
        if (size < 0) {
            throw new IOException("Invalid read size: " + size);
        }
        ByteBuffer buffer = ByteBuffer.allocate(size);
        while (buffer.hasRemaining()) {
            int read = channel.read(buffer);
            if (read < 0) {
                throw new IOException("Connection closed by broker while reading response");
            }
        }
        buffer.flip();
        return buffer;
    }

    /**
     * Get metadata for all topics
     */
    public Map<String, TopicMetadata> getTopicMetadata() {
        return new HashMap<>(topicMetadata);
    }

    /**
     * Get metadata for a specific topic
     */
    public TopicMetadata getTopicMetadata(String topic) {
        return topicMetadata.get(topic);
    }

    /**
     * Get broker information
     */
    public Map<Integer, BrokerInfo> getBrokers() {
        return new HashMap<>(brokers);
    }

    /**
     * Topic metadata
     */
    public static class TopicMetadata {
        private final String name;
        private final List<PartitionInfo> partitions;

        public TopicMetadata(String name, List<PartitionInfo> partitions) {
            this.name = name;
            this.partitions = partitions;
        }

        public String getName() {
            return name;
        }

        public List<PartitionInfo> getPartitions() {
            return new ArrayList<>(partitions);
        }

        @Override
        public String toString() {
            return "TopicMetadata{name='" + name + "', partitions=" + partitions + "}";
        }
    }

    /**
     * Partition information
     */
    public static class PartitionInfo {
        private final int id;
        private final int leader;
        private final List<Integer> followers;

        public PartitionInfo(int id, int leader, List<Integer> followers) {
            this.id = id;
            this.leader = leader;
            this.followers = followers;
        }

        public int getId() {
            return id;
        }

        public int getLeader() {
            return leader;
        }

        public List<Integer> getFollowers() {
            return new ArrayList<>(followers);
        }

        @Override
        public String toString() {
            return "PartitionInfo{id=" + id + ", leader=" + leader + ", followers=" + followers + "}";
        }
    }
}
