package com.simplekafka.broker;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.zookeeper.KeeperException;

/**
 * A simplified implementation of a Kafka-like broker
 */
public class SimpleKafkaBroker {
    private static final Logger LOGGER = Logger.getLogger(SimpleKafkaBroker.class.getName());
    private static final String DATA_DIR = "data";
    private static final int READ_BUFFER_SIZE = 64 * 1024;
    /** 控制面（broker 间转发）连接超时。 */
    private static final int CONTROL_CONNECT_TIMEOUT_MS = 3000;
    /** 控制面读超时：对端不响应时必须能超时退出，不能永久占着工作线程。 */
    private static final int CONTROL_READ_TIMEOUT_MS = 5000;
    /**
     * 判定 broker「确认下线」前允许的缺席时长。
     *
     * <p>ZK 临时节点靠会话超时自然清理（最长 30 秒），刚重启的 broker 会短暂不在
     * {@code /brokers} 里；没有这个宽限期，一重启就会把在线副本当成死节点剔除。
     */
    private static final long BROKER_ABSENCE_GRACE_MS = 15_000L;

    private final int brokerId;
    private final String brokerHost;
    private final int brokerPort;
    private final Map<String, List<Partition>> topics;
    private final ExecutorService executor;
    private final ServerSocketChannel serverChannel;
    private final AtomicBoolean isRunning;
    private final AtomicBoolean isController;
    private final Map<Integer, BrokerInfo> clusterMetadata;
    /** 每个 broker id 最近一次出现在 ZooKeeper 中的时间，用于判定「确认下线」。 */
    private final Map<Integer, Long> brokerLastSeen;
    /** 本 broker 的启动时刻，用于「从未见过的副本」的启动宽限期判定。 */
    private final long startTimeMs;
    private final ZookeeperClient zkClient;
    private Thread registrationKeeper;

    public SimpleKafkaBroker(int brokerId, String host, int port, int zkPort) throws IOException {
        this.brokerId = brokerId;
        this.brokerHost = host;
        this.brokerPort = port;
        this.topics = new ConcurrentHashMap<>();
        // 用可增长的线程池：单个卡住的连接不能把固定线程池占满。
        // （曾经因为固定 10 个线程被占满，导致 broker 连元数据请求都无法响应 = “假死”）
        // 注意：这里必须是**非守护线程**，否则 main 返回后 JVM 会直接退出。
        this.executor = new ThreadPoolExecutor(8, 200, 60L, TimeUnit.SECONDS,
                new SynchronousQueue<>(), runnable -> new Thread(runnable, "broker-client-" + brokerId));
        this.serverChannel = ServerSocketChannel.open();
        this.isRunning = new AtomicBoolean(false);
        this.isController = new AtomicBoolean(false);
        this.clusterMetadata = new ConcurrentHashMap<>();
        this.brokerLastSeen = new ConcurrentHashMap<>();
        this.startTimeMs = System.currentTimeMillis();

        // Initialize data directory
        File dataDir = new File(DATA_DIR + File.separator + brokerId);
        if (!dataDir.exists()) {
            dataDir.mkdirs();
        }

        // Initialize ZooKeeper client
        this.zkClient = new ZookeeperClient("localhost", zkPort);
    }

    /**
     * Notify a broker about topic creation
     */
    private void notifyBrokerForTopicCreation(int brokerId, String topic) {
        BrokerInfo broker = clusterMetadata.get(brokerId);
        if (broker == null)
            return;

        executor.submit(() -> {
            try (SocketChannel brokerChannel = SocketChannel.open()) {
                brokerChannel.connect(new InetSocketAddress(broker.getHost(), broker.getPort()));

                // Prepare notification
                ByteBuffer request = ByteBuffer.allocate(3 + topic.length());
                request.put(Protocol.TOPIC_NOTIFICATION);
                request.putShort((short) topic.length());
                request.put(topic.getBytes());
                request.flip();

                // Send notification
                brokerChannel.write(request);

                // Read acknowledgment
                ByteBuffer response = ByteBuffer.allocate(1);
                brokerChannel.read(response);
            } catch (IOException e) {
                LOGGER.log(Level.WARNING, "Failed to notify broker " + brokerId + " about topic creation", e);
            }
        });
    }

    /**
     * Handle topic notification from controller
     */
    private void handleTopicNotification(SocketChannel clientChannel, ByteBuffer buffer) throws IOException {
        short topicLength = buffer.getShort();
        byte[] topicBytes = new byte[topicLength];
        buffer.get(topicBytes);
        String topic = new String(topicBytes);

        LOGGER.info("Received topic notification for: " + topic);

        // Load topic metadata from ZooKeeper
        try {
            loadTopic(topic);

            // Send acknowledgment
            ByteBuffer response = ByteBuffer.allocate(1);
            response.put((byte) 0); // Acknowledgment
            response.flip();
            clientChannel.write(response);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to load topic: " + topic, e);

            // Send error response
            ByteBuffer response = ByteBuffer.allocate(1);
            response.put((byte) 1); // Error
            response.flip();
            clientChannel.write(response);
        }
    }

    /**
     * 从 ZooKeeper 加载或刷新 topic 元数据。
     *
     * <p>与最初版本不同，这里对已加载的 topic 也会**刷新** leader / followers：
     * controller 在节点增减后会重新分配分区，如果 broker 一直沿用启动时缓存的 leader，
     * 就会把请求转发给早已下线的节点（表现为 "Leader broker not found: 2"）。
     */
    private synchronized void loadTopic(String topic) throws Exception {
        String topicPath = "/topics/" + topic;
        if (!zkClient.exists(topicPath)) {
            throw new Exception("Topic does not exist in ZooKeeper: " + topic);
        }

        String topicDir = DATA_DIR + File.separator + brokerId + File.separator + topic;
        new File(topicDir).mkdirs();

        List<String> partitionIds = zkClient.getChildren(topicPath + "/partitions");
        List<Partition> partitions = topics.get(topic);
        if (partitions == null) {
            partitions = new CopyOnWriteArrayList<>();
            topics.put(topic, partitions);
        }

        for (String partitionId : partitionIds) {
            int id = Integer.parseInt(partitionId);
            String partitionPath = topicPath + "/partitions/" + partitionId;
            String partitionData = zkClient.getData(partitionPath);

            String[] parts = partitionData.split(";");
            int leader = Integer.parseInt(parts[0]);

            List<Integer> followers = new ArrayList<>();
            if (parts.length > 1 && !parts[1].isEmpty()) {
                String[] followerIds = parts[1].split(",");
                for (String followerId : followerIds) {
                    if (!followerId.isEmpty()) {
                        followers.add(Integer.parseInt(followerId));
                    }
                }
            }

            Partition existing = findPartition(partitions, id);
            if (existing == null) {
                String partitionDir = topicDir + File.separator + id;
                new File(partitionDir).mkdirs();
                partitions.add(new Partition(id, leader, followers, partitionDir));
                LOGGER.info("Loaded partition " + id + " for topic " + topic +
                        ", leader: " + leader + ", followers: " + followers);
            } else if (existing.getLeader() != leader
                    || !new HashSet<>(existing.getFollowers()).equals(new HashSet<>(followers))) {
                existing.setLeader(leader);
                existing.setFollowers(followers);
                LOGGER.info("Refreshed partition " + id + " of topic " + topic +
                        ": leader=" + leader + ", followers=" + followers);
            }
        }

        // 移除 ZooKeeper 中已不存在的分区
        for (Partition partition : new ArrayList<>(partitions)) {
            if (!partitionIds.contains(String.valueOf(partition.getId()))) {
                partition.close();
                partitions.remove(partition);
            }
        }

        LOGGER.info("Topic " + topic + " ready with " + partitions.size() + " partitions");
    }

    /**
     * 重新读取所有 topic 的分区元数据，刷新本地缓存的 leader/followers。
     *
     * <p>调用场景：注册保活线程的周期检查、{@code /topics} 子节点变化、以及请求发现
     * leader 已不在集群时的按需刷新。
     */
    private synchronized void refreshTopicsFromZookeeper() {
        try {
            if (!zkClient.exists("/topics")) return;
            for (String topic : zkClient.getChildren("/topics")) {
                loadTopic(topic);
            }
        } catch (Exception e) {
            LOGGER.log(Level.FINE, "Failed to refresh topics from ZooKeeper", e);
        }
    }

    private static Partition findPartition(List<Partition> partitions, int partitionId) {
        if (partitions == null) return null;
        for (Partition partition : partitions) {
            if (partition.getId() == partitionId) return partition;
        }
        return null;
    }

    /**
     * 取出分区，必要时先从 ZooKeeper 刷新元数据。
     *
     * <p>三种需要刷新的情况：本 broker 还没加载过该 topic（创建时它不在线）、
     * 新增了分区、以及缓存的 leader 已经不在集群中（controller 重新分配过）。
     */
    private Partition requirePartition(String topic, int partitionId) throws Exception {
        Partition partition = findPartition(topics.get(topic), partitionId);
        if (partition == null || !clusterMetadata.containsKey(partition.getLeader())) {
            loadTopic(topic);
            partition = findPartition(topics.get(topic), partitionId);
        }
        if (partition == null) {
            throw new Exception("Partition does not exist: " + topic + ":" + partitionId);
        }
        return partition;
    }

    /**
     * Load all topics from ZooKeeper
     */
    public void loadTopics() {
        try {
            List<String> topicNames = zkClient.getChildren("/topics");

            for (String topic : topicNames) {
                try {
                    loadTopic(topic);
                } catch (Exception e) {
                    LOGGER.log(Level.SEVERE, "Failed to load topic: " + topic, e);
                }
            }

            LOGGER.info("Loaded " + topics.size() + " topics");
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to load topics", e);
        }
    }

    /**
     * Main entry point for running a broker
     */
    public static void main(String[] args) {
        if (args.length < 3) {
            System.out.println("Usage: SimpleKafkaBroker <brokerId> <host> <port> [zkPort]");
            System.exit(1);
        }

        try {
            int brokerId = Integer.parseInt(args[0]);
            String host = args[1];
            int port = Integer.parseInt(args[2]);
            int zkPort = args.length > 3 ? Integer.parseInt(args[3]) : 2181;

            SimpleKafkaBroker broker = new SimpleKafkaBroker(brokerId, host, port, zkPort);
            broker.start();

            // Add shutdown hook: 清理后强制退出，避免 ZooKeeper 客户端等线程阻塞导致进程僵留
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                broker.stop();
                Runtime.getRuntime().halt(0);
            }));

            System.out.println("SimpleKafka broker started. Press Ctrl+C to stop.");

            // 显式保活：main 一旦返回，JVM 就可能在只剩守护线程时退出，
            // 表现为“刚打印启动完成就 Stopping...”。
            while (broker.isRunning()) {
                Thread.sleep(200L);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to start broker", e);
        }
    }

    /** 是否仍在运行（供 main 保活与测试使用）。 */
    public boolean isRunning() {
        return isRunning.get();
    }

    /**
     * Start the broker server
     */
    public void start() throws IOException {
        if (isRunning.compareAndSet(false, true)) {
            // Bind to socket
            serverChannel.socket().bind(new InetSocketAddress(brokerHost, brokerPort));
            serverChannel.configureBlocking(false);

            LOGGER.info("SimpleKafka broker started on " + brokerHost + ":" + brokerPort);

            // Register with ZooKeeper
            registerWithZookeeper();

            // Start controller election process
            electController();

            // Load existing topics
            loadTopics();

            // Keep the ZooKeeper registration alive: a restart right after a hard kill may
            // find a stale ephemeral node from the previous session, and the node could also
            // be removed from the dashboard. Without this, the broker stays invisible.
            startRegistrationKeeper();

            // Accept client connections
            executor.submit(this::acceptConnections);
        }
    }

    /**
     * Stop the broker server
     */
    public void stop() {
        if (isRunning.compareAndSet(true, false)) {
            try {
                LOGGER.info("Stopping SimpleKafka broker...");

                if (registrationKeeper != null) {
                    registrationKeeper.interrupt();
                }

                // Close server socket
                serverChannel.close();

                // Close all topic partitions
                for (List<Partition> partitions : topics.values()) {
                    for (Partition partition : partitions) {
                        partition.close();
                    }
                }

                // Shut down executor
                executor.shutdownNow();

                // Close ZooKeeper connection
                closeZooKeeperBounded();

                LOGGER.info("SimpleKafka broker stopped");
            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, "Error stopping broker", e);
            }
        }
    }

    /**
     * 限时关闭 ZooKeeper 连接。
     *
     * <p>ZooKeeper 的 {@code close()} 在服务端已不可达时可能长时间阻塞，如果直接在 shutdown hook 里调用，
     * 会导致进程收到 SIGTERM 后永不退出（表现为“停止全部”无效）。因此这里放到守护线程里等待最多 2 秒，
     * 超时就放弃——会话本身也会随进程退出而失效。
     */
    private void closeZooKeeperBounded() {
        Thread closer = new Thread(() -> {
            try {
                zkClient.close();
            } catch (Exception ignored) {
                // 停机阶段的关闭失败无需处理
            }
        }, "zk-close-" + brokerId);
        closer.setDaemon(true);
        closer.start();
        try {
            closer.join(2000L);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Register this broker with ZooKeeper
     */
    private void registerWithZookeeper() {
        try {
            zkClient.connect();
            String brokerPath = "/brokers/" + brokerId;
            String brokerData = brokerHost + ":" + brokerPort;
            zkClient.createEphemeralNode(brokerPath, brokerData);

            // Add broker info to local metadata
            BrokerInfo selfInfo = new BrokerInfo(brokerId, brokerHost, brokerPort);
            clusterMetadata.put(brokerId, selfInfo);
            brokerLastSeen.put(brokerId, System.currentTimeMillis());

            // Watch for other brokers
            zkClient.watchChildren("/brokers", this::onBrokersChanged);

            // 监听 topic 变化：任何新 topic 出现都立即加载，避免“ZooKeeper 里有、broker 本地没有”
            zkClient.watchChildren("/topics", ignored -> refreshTopicsFromZookeeper());

            LOGGER.info("Registered with ZooKeeper at " + zkClient.getConnectString());
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to register with ZooKeeper", e);
        }
    }

    /**
     * 周期性检查并修复自身在 ZooKeeper 中的注册状态。
     *
     * <p>需要它的原因：
     * <ul>
     *   <li>broker 被强杀后立刻重启时，上一个会话的临时节点可能还没过期（最长 30 秒），
     *       导致新实例的注册被拒绝，此后该 broker 会一直“隐身”；</li>
     *   <li>通过可视化界面手动删除了 {@code /brokers/<id>} 节点；</li>
     *   <li>controller 节点意外消失时需要重新参与选举。</li>
     * </ul>
     * 因此这里每 5 秒检查一次，发现缺失就补建，保证节点数量可以自由增减且始终可见。
     */
    private void startRegistrationKeeper() {
        registrationKeeper = new Thread(() -> {
            String brokerPath = "/brokers/" + brokerId;
            while (isRunning.get()) {
                try {
                    Thread.sleep(5000L);
                    if (!isRunning.get()) {
                        return;
                    }
                    if (!zkClient.isConnected()) {
                        LOGGER.warning("ZooKeeper connection lost, reconnecting broker " + brokerId);
                        zkClient.connect();
                        zkClient.watchChildren("/brokers", this::onBrokersChanged);
                    }
                    if (!zkClient.exists(brokerPath)) {
                        zkClient.createEphemeralNode(brokerPath, brokerHost + ":" + brokerPort);
                        LOGGER.warning("Re-registered broker " + brokerId + " with ZooKeeper at " + brokerPath);
                    }
                    // 周期刷新分区元数据：controller 重新分配 leader/follower 后，本 broker 能自动跟上
                    refreshTopicsFromZookeeper();
                    if (!zkClient.exists("/controller")) {
                        electController();
                    }
                    // controller 周期做一次再平衡：
                    // ① 宽限期到期后的真下线副本能在下一次检查里被清理；
                    // ② 万一漏掉了某个 watch 事件，也能自愈合。
                    if (amController()) {
                        rebalancePartitions();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                } catch (Exception e) {
                    LOGGER.log(Level.FINE, "registration keeper check failed for broker " + brokerId, e);
                }
            }
        }, "broker-registration-keeper-" + brokerId);
        registrationKeeper.setDaemon(true);
        registrationKeeper.start();
    }

    /**
     * Handle changes in the broker list from ZooKeeper
     */
    private void onBrokersChanged(List<String> brokerIds) {
        LOGGER.info("Broker change detected. Current brokers: " + brokerIds);

        // 记录心跳时间：rebalance 靠"曾经见过 + 缺席超过宽限期"才能确认一个 broker 真的下线
        long now = System.currentTimeMillis();
        for (String id : brokerIds) {
            try {
                brokerLastSeen.put(Integer.parseInt(id), now);
            } catch (NumberFormatException ignored) {
                // 非数字子节点直接忽略
            }
        }

        // Update cluster metadata
        for (String id : brokerIds) {
            try {
                int brokerId = Integer.parseInt(id);
                if (!clusterMetadata.containsKey(brokerId)) {
                    String brokerData = zkClient.getData("/brokers/" + id);
                    String[] hostPort = brokerData.split(":");
                    BrokerInfo info = new BrokerInfo(
                            brokerId,
                            hostPort[0],
                            Integer.parseInt(hostPort[1]));
                    clusterMetadata.put(brokerId, info);
                    LOGGER.info("Added broker: " + info);
                }
            } catch (Exception e) {
                LOGGER.log(Level.WARNING, "Failed to process broker info", e);
            }
        }

        // Remove brokers that have disappeared
        List<Integer> toRemove = new ArrayList<>();
        for (Integer brokerId : clusterMetadata.keySet()) {
            if (!brokerIds.contains(String.valueOf(brokerId))) {
                toRemove.add(brokerId);
            }
        }

        for (Integer brokerId : toRemove) {
            clusterMetadata.remove(brokerId);
            LOGGER.info("Removed broker: " + brokerId);
        }

        // Re-elect controller if needed
        if (!brokerIds.contains(String.valueOf(brokerId)) && isController.get()) {
            isController.set(false);
            LOGGER.info("This broker is no longer in the cluster, giving up controller status");
        } else if (amController()) {
            // As controller, rebalance partitions due to cluster changes
            rebalancePartitions();
        } else {
            // Re-attempt controller election
            electController();
        }
    }

    /**
     * Participate in controller election
     */
    private void electController() {
        try {
            String controllerPath = "/controller";

            // First, make sure the node doesn't already exist (or is empty)
            boolean nodeExists = zkClient.exists(controllerPath);
            if (nodeExists) {
                String existingData = zkClient.getData(controllerPath);
                if (existingData == null || existingData.trim().isEmpty()) {
                    // Node exists but has empty data, try to delete it
                    zkClient.deleteNode(controllerPath);
                    nodeExists = false;
                    LOGGER.info("Deleted empty controller node");
                }
            }

            // Now try to create the node
            boolean becameController = false;
            if (!nodeExists) {
                try {
                    becameController = zkClient.createEphemeralNode(controllerPath, String.valueOf(brokerId));
                } catch (KeeperException.NodeExistsException e) {
                    // 启动时多个 broker 同时竞选，别人抢先创建是正常竞争，不是错误
                    becameController = false;
                }
            }

            if (becameController) {
                isController.set(true);
                LOGGER.info("This broker is now the active controller");

                // As controller, ensure all topics are properly replicated
                rebalancePartitions();
            } else {
                // Double-check the data
                String controllerId = zkClient.getData(controllerPath);
                if (controllerId == null || controllerId.trim().isEmpty()) {
                    LOGGER.warning("Controller node exists but has no data. This is unexpected.");
                    // Try again after a delay
                    new Thread(() -> {
                        try {
                            Thread.sleep(1000);
                            electController();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }).start();
                    return;
                }

                LOGGER.info("Current controller is broker " + controllerId);

                // Watch controller node for changes
                zkClient.watchNode(controllerPath, this::onControllerChange);
            }
        } catch (KeeperException.NodeExistsException e) {
            // 竞选失败（别人先建成功）属于正常竞争：读取当前 controller 并监听即可
            onControllerChange();
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Controller election failed: " + e, e);

            // Try again after a delay
            new Thread(() -> {
                try {
                    Thread.sleep(2000);
                    electController();
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            }).start();
        }
    }

    /**
     * Handle controller change notification
     */
    private void onControllerChange() {
        LOGGER.info("Controller changed, initiating new election");
        electController();
    }

    /**
     * 我是不是 controller —— **以 ZooKeeper 的 {@code /controller} 为准**，并顺手校准本地缓存。
     *
     * <p>本地 {@code isController} 只是缓存：选举竞争、watch 事件丢失、控制台删改节点等
     * 都会让它与 ZooKeeper 不一致。而「本地说不是、ZooKeeper 说是」正是历史上创建 topic
     * 自环转发的根因，所以每次判断都向 ZooKeeper 确认一次。
     */
    private boolean amController() {
        try {
            String data = zkClient.getData("/controller");
            if (data == null || data.trim().isEmpty()) {
                isController.set(false);
                return false;
            }
            boolean mine = Integer.parseInt(data.trim()) == brokerId;
            isController.set(mine);
            return mine;
        } catch (Exception e) {
            // ZooKeeper 暂时读不到时退回缓存值
            return isController.get();
        }
    }

    /**
     * 重新分配分区副本。
     *
     * <p>一个副本只有在「不在 ZooKeeper 实时 {@code /brokers} 子节点里」**且**
     * 「已经缺席超过 {@link #BROKER_ABSENCE_GRACE_MS}」时才算确认下线；
     * 只满足一条就保持原分配不动，等下一次 watch 或 5 秒周期检查再判断。
     *
     * <p>早期版本只用本地 {@code clusterMetadata} 作为存活依据，冷启动时最先拉起的 broker
     * 当上 controller 时本地还没见过同伴，会把在线副本误删，导致副本数被缩成 1、复制静默失效。
     *
     * <p>真的需要换主时，保留原副本列表里的存活节点及其顺序，
     * 所以「第一个 follower 接任 leader」这个直观行为得以保持。
     */
    private void rebalancePartitions() {
        // 以 ZooKeeper 为真相源：本地标记过时也不会漏掉一次该做的再平衡
        if (!amController()) {
            return;
        }

        LOGGER.info("Rebalancing partitions across cluster");
        try {
            // 以 ZooKeeper 的实时注册为准；读不到时退回本地缓存
            List<Integer> liveBrokers = new ArrayList<>();
            try {
                for (String id : zkClient.getChildren("/brokers")) {
                    liveBrokers.add(Integer.parseInt(id));
                }
            } catch (Exception e) {
                LOGGER.log(Level.WARNING, "Failed to read live brokers from ZooKeeper", e);
            }
            if (liveBrokers.isEmpty()) {
                liveBrokers.addAll(clusterMetadata.keySet());
            }
            Collections.sort(liveBrokers);
            if (liveBrokers.isEmpty()) {
                return;
            }

            long now = System.currentTimeMillis();
            for (String topic : zkClient.getChildren("/topics")) {
                String partitionsPath = "/topics/" + topic + "/partitions";
                if (!zkClient.exists(partitionsPath)) continue;

                for (String partitionId : zkClient.getChildren(partitionsPath)) {
                    String node = partitionsPath + "/" + partitionId;
                    String[] value = zkClient.getData(node).split(";", 2);

                    List<Integer> replicas = new ArrayList<>();
                    replicas.add(Integer.parseInt(value[0]));
                    if (value.length > 1 && !value[1].isEmpty()) {
                        for (String id : value[1].split(",")) {
                            if (!id.isEmpty()) {
                                replicas.add(Integer.parseInt(id));
                            }
                        }
                    }

                    // 没有任何「确认下线」的副本 ⇒ 分配原样保留
                    boolean anyConfirmedGone = false;
                    for (int replica : replicas) {
                        if (isConfirmedGone(replica, liveBrokers, now)) {
                            anyConfirmedGone = true;
                            break;
                        }
                    }
                    if (!anyConfirmedGone) {
                        // 有副本不在线但还没过宽限期（或从未见过）：只记录，等下一轮再看
                        List<Integer> missing = new ArrayList<>();
                        for (int replica : replicas) {
                            if (!liveBrokers.contains(replica)) {
                                missing.add(replica);
                            }
                        }
                        if (!missing.isEmpty()) {
                            LOGGER.info("Partition " + topic + "-" + partitionId + " keeps missing replica(s) "
                                    + missing + " within grace period (" + BROKER_ABSENCE_GRACE_MS + " ms)");
                        }
                        continue;
                    }

                    int replicaCount = Math.max(1, replicas.size());
                    List<Integer> reassigned = new ArrayList<>();
                    for (int replica : replicas) {
                        if (!isConfirmedGone(replica, liveBrokers, now) && !reassigned.contains(replica)) {
                            reassigned.add(replica);
                        }
                    }
                    if (reassigned.isEmpty()) {
                        reassigned.add(liveBrokers.get(0));
                    }
                    // 用其它存活 broker 补齐到原来的副本数量
                    for (int broker : liveBrokers) {
                        if (reassigned.size() >= replicaCount) break;
                        if (!reassigned.contains(broker)) reassigned.add(broker);
                    }

                    StringBuilder data = new StringBuilder(String.valueOf(reassigned.get(0))).append(';');
                    for (int i = 1; i < reassigned.size(); i++) {
                        data.append(reassigned.get(i)).append(',');
                    }
                    zkClient.setData(node, data.toString());
                    LOGGER.info("Reassigned " + topic + "-" + partitionId
                            + " (was " + replicas + ") → " + reassigned);
                }
                loadTopic(topic);
            }
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Rebalance failed", e);
        }
    }

    /**
     * 是否「确认」某 broker 已下线。
     *
     * <p>两种情形：
     * <ol>
     *   <li><b>见过它</b>：它不在实时存活集合里，且已经缺席超过宽限期；</li>
     *   <li><b>本次启动从未见过它</b>：启动后的前 {@link #BROKER_ABSENCE_GRACE_MS} 毫秒内保持保守
     *       （同伴可能还在启动过程中），之后就以 ZooKeeper 为准 —— 否则带上“永久死副本”
     *       重启一次后，这个死副本会永远留在分配里，没人会去清它。</li>
     * </ol>
     */
    private boolean isConfirmedGone(int id, List<Integer> liveBrokers, long now) {
        if (liveBrokers.contains(id)) {
            return false;
        }
        Long lastSeen = brokerLastSeen.get(id);
        if (lastSeen != null) {
            return now - lastSeen > BROKER_ABSENCE_GRACE_MS;
        }
        return now - startTimeMs > BROKER_ABSENCE_GRACE_MS;
    }

    /**
     * Update partition metadata in ZooKeeper
     */
    private void updatePartitionMetadata(String topic, Partition partition) {
        try {
            String path = "/topics/" + topic + "/partitions/" + partition.getId();
            String data = partition.getLeader() + ";";
            for (int follower : partition.getFollowers()) {
                data += follower + ",";
            }

            if (zkClient.exists(path)) {
                zkClient.setData(path, data);
            } else {
                zkClient.createPersistentNode(path, data);
            }
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to update partition metadata", e);
        }
    }

    /**
     * Accept client connections
     */
    private void acceptConnections() {
        while (isRunning.get()) {
            try {
                SocketChannel clientChannel = serverChannel.accept();
                if (clientChannel != null) {
                    clientChannel.configureBlocking(false);
                    LOGGER.info("Accepted connection from " + clientChannel.getRemoteAddress());

                    // Handle client connection in a separate thread
                    executor.submit(() -> handleClient(clientChannel));
                }

                Thread.sleep(100); // Small pause to prevent CPU spin
            } catch (Exception e) {
                if (isRunning.get()) {
                    LOGGER.log(Level.SEVERE, "Error accepting connection", e);
                }
            }
        }
    }

    /**
     * Handle client connection
     */
    private void handleClient(SocketChannel clientChannel) {
        try {
            ByteBuffer buffer = ByteBuffer.allocate(READ_BUFFER_SIZE);

            while (clientChannel.isOpen() && isRunning.get()) {
                buffer.clear();
                int bytesRead = clientChannel.read(buffer);

                if (bytesRead > 0) {
                    buffer.flip();
                    // Process the message based on protocol
                    processClientMessage(clientChannel, buffer);
                } else if (bytesRead < 0) {
                    // Connection closed by client
                    clientChannel.close();
                    break;
                }

                Thread.sleep(50); // Small pause to prevent CPU spin
            }
        } catch (Exception e) {
            if (isRunning.get()) {
                LOGGER.log(Level.SEVERE, "Error handling client", e);
            }
        } finally {
            try {
                if (clientChannel.isOpen()) {
                    clientChannel.close();
                }
            } catch (IOException e) {
                LOGGER.log(Level.WARNING, "Error closing client channel", e);
            }
        }
    }

    /**
     * Process client message based on SimpleKafka wire protocol
     */
    private void processClientMessage(SocketChannel clientChannel, ByteBuffer buffer) throws IOException {
        byte messageType = buffer.get();

        switch (messageType) {
            case Protocol.PRODUCE:
                handleProduceRequest(clientChannel, buffer);
                break;
            case Protocol.FETCH:
                handleFetchRequest(clientChannel, buffer);
                break;
            case Protocol.METADATA:
                handleMetadataRequest(clientChannel, buffer);
                break;
            case Protocol.CREATE_TOPIC:
                handleCreateTopicRequest(clientChannel, buffer);
                break;
            case Protocol.REPLICATE:
                handleReplicateRequest(clientChannel, buffer);
                break;
            case Protocol.TOPIC_NOTIFICATION:
                handleTopicNotification(clientChannel, buffer);
                break;
            default:
                LOGGER.warning("Unknown message type: " + messageType);
                Protocol.sendErrorResponse(clientChannel, "Unknown message type");
        }
    }

    /**
     * Handle produce request from client
     */
    private void handleProduceRequest(SocketChannel clientChannel, ByteBuffer buffer) throws IOException {
        short topicLength = buffer.getShort();
        byte[] topicBytes = new byte[topicLength];
        buffer.get(topicBytes);
        String topic = new String(topicBytes);

        int partition = buffer.getInt();
        int messageSize = buffer.getInt();
        byte[] message = new byte[messageSize];
        buffer.get(message);

        LOGGER.info("Produce request for topic: " + topic + ", partition: " + partition);

        // 取出分区；必要时先从 ZooKeeper 刷新，避免使用失效的 leader 信息
        Partition targetPartition;
        try {
            targetPartition = requirePartition(topic, partition);
        } catch (Exception e) {
            Protocol.sendErrorResponse(clientChannel, e.getMessage() == null ? "Partition does not exist" : e.getMessage());
            return;
        }

        // Check if this broker is the leader for the partition
        if (targetPartition.getLeader() != brokerId) {
            // Forward to leader
            forwardProduceToLeader(clientChannel, topic, partition, message, targetPartition.getLeader());
            return;
        }

        // Append message to log
        long offset = targetPartition.append(message);

        // Replicate to followers
        replicateToFollowers(topic, targetPartition, message, offset);

        // Send acknowledgment to client
        ByteBuffer response = ByteBuffer.allocate(10);
        response.put(Protocol.PRODUCE_RESPONSE);
        response.putLong(offset);
        response.put((byte) (offset > -1 ? 0 : 1)); // 0 = success, 1 = error
        response.flip();
        clientChannel.write(response);
    }

    /**
     * Forward produce request to leader broker
     */
    private void forwardProduceToLeader(SocketChannel clientChannel, String topic, int partition,
            byte[] message, int leaderId) throws IOException {
        BrokerInfo leader = clusterMetadata.get(leaderId);
        if (leader == null) {
            Protocol.sendErrorResponse(clientChannel, "Leader broker not available");
            return;
        }

        try (SocketChannel leaderChannel = SocketChannel.open()) {
            leaderChannel.connect(new InetSocketAddress(leader.getHost(), leader.getPort()));

            // Prepare forwarded produce request
            // 字段长度：type(1) + topicLen(2) + topic(N) + partition(4) + msgLen(4) + msg(M)
            ByteBuffer request = ByteBuffer.allocate(11 + topic.length() + message.length);
            request.put(Protocol.PRODUCE);
            request.putShort((short) topic.length());
            request.put(topic.getBytes());
            request.putInt(partition);
            request.putInt(message.length);
            request.put(message);
            request.flip();

            // Send request to leader
            leaderChannel.write(request);

            // Read response from leader
            ByteBuffer response = ByteBuffer.allocate(10);
            leaderChannel.read(response);
            response.flip();

            // Forward leader's response back to client
            clientChannel.write(response);
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Failed to forward produce request to leader", e);
            Protocol.sendErrorResponse(clientChannel, "Failed to forward to leader");
        }
    }

    /**
     * Replicate message to follower brokers
     */
    private void replicateToFollowers(String topic, Partition partition, byte[] message, long offset) {
        List<Integer> followers = partition.getFollowers();
        LOGGER.info("Replicating " + topic + "-" + partition.getId() + " offset=" + offset
                + " to followers=" + followers + " knownBrokers=" + clusterMetadata.keySet());

        for (int followerId : followers) {
            if (followerId == brokerId)
                continue; // Skip self

            BrokerInfo follower = clusterMetadata.get(followerId);
            if (follower == null) {
                LOGGER.warning("Skip replication to unknown broker " + followerId);
                continue;
            }

            executor.submit(() -> {
                try (SocketChannel followerChannel = SocketChannel.open()) {
                    followerChannel.connect(new InetSocketAddress(follower.getHost(), follower.getPort()));

                    // Prepare replication request
                    // 字段长度：type(1) + topicLen(2) + topic(N) + partition(4) + offset(8) + msgLen(4) + msg(M)
                    ByteBuffer request = ByteBuffer.allocate(19 + topic.length() + message.length);
                    request.put(Protocol.REPLICATE);
                    request.putShort((short) topic.length());
                    request.put(topic.getBytes());
                    request.putInt(partition.getId());
                    request.putLong(offset);
                    request.putInt(message.length);
                    request.put(message);
                    request.flip();

                    // Send request to follower
                    followerChannel.write(request);

                    // Read acknowledgment
                    ByteBuffer response = ByteBuffer.allocate(1);
                    followerChannel.read(response);
                    response.flip();

                    byte ack = response.get();
                    LOGGER.info("Replication to follower " + followerId + " " +
                            (ack == Protocol.REPLICATE_ACK ? "succeeded" : "failed"));
                } catch (Exception e) {
                    // 必须捕获 Exception 而不只是 IOException：解析类的 RuntimeException 以前会被
                    // 线程池静默吞掉，导致复制失败却没有任何日志（这正是一次真实故障的成因）。
                    LOGGER.log(Level.SEVERE, "Replication to follower " + followerId + " failed: " + e, e);
                }
            });
        }
    }

    /**
     * Handle replication request from leader
     */
    private void handleReplicateRequest(SocketChannel clientChannel, ByteBuffer buffer) throws IOException {
        short topicLength = buffer.getShort();
        byte[] topicBytes = new byte[topicLength];
        buffer.get(topicBytes);
        String topic = new String(topicBytes);

        int partitionId = buffer.getInt();
        long offset = buffer.getLong();
        int messageSize = buffer.getInt();
        byte[] message = new byte[messageSize];
        buffer.get(message);

        LOGGER.info("Replication request for topic: " + topic + ", partition: " + partitionId + ", offset: " + offset);

        // 取出分区；必要时先从 ZooKeeper 刷新
        Partition targetPartition;
        try {
            targetPartition = requirePartition(topic, partitionId);
        } catch (Exception e) {
            ByteBuffer response = ByteBuffer.allocate(1);
            response.put((byte) 0); // Failed
            response.flip();
            clientChannel.write(response);
            return;
        }

        // Append message to log (as follower)，按 leader 分配的 offset 写入，保证副本之间 offset 对齐
        targetPartition.appendAtOffset(offset, message);

        // Send acknowledgment
        ByteBuffer response = ByteBuffer.allocate(1);
        response.put(Protocol.REPLICATE_ACK);
        response.flip();
        clientChannel.write(response);
    }

    /**
     * Handle fetch request from client
     */
    private void handleFetchRequest(SocketChannel clientChannel, ByteBuffer buffer) throws IOException {
        short topicLength = buffer.getShort();
        byte[] topicBytes = new byte[topicLength];
        buffer.get(topicBytes);
        String topic = new String(topicBytes);

        int partition = buffer.getInt();
        long offset = buffer.getLong();
        int maxBytes = buffer.getInt();

        LOGGER.info("Fetch request for topic: " + topic + ", partition: " + partition +
                ", offset: " + offset + ", maxBytes: " + maxBytes);

        // 取出分区；必要时先从 ZooKeeper 刷新
        Partition targetPartition;
        try {
            targetPartition = requirePartition(topic, partition);
        } catch (Exception e) {
            Protocol.sendErrorResponse(clientChannel, e.getMessage() == null ? "Partition does not exist" : e.getMessage());
            return;
        }

        // Check if the offset is valid
        if (offset >= targetPartition.getLogEndOffset()) {
            // No messages available at this offset
            ByteBuffer response = ByteBuffer.allocate(5);
            response.put(Protocol.FETCH_RESPONSE);
            response.putInt(0); // 0 messages
            response.flip();
            writeFully(clientChannel, response);
            return;
        }

        // Read messages from log
        List<byte[]> messages = targetPartition.readMessages(offset, maxBytes);

        // Send response
        int totalSize = 5; // 1 byte for response type, 4 bytes for message count
        for (byte[] msg : messages) {
            totalSize += 12 + msg.length; // 8 bytes for offset, 4 bytes for length, plus message bytes
        }

        ByteBuffer response = ByteBuffer.allocate(totalSize);
        response.put(Protocol.FETCH_RESPONSE);
        response.putInt(messages.size());

        long currentOffset = offset;
        for (byte[] msg : messages) {
            response.putLong(currentOffset);
            response.putInt(msg.length);
            response.put(msg);
            currentOffset++;
        }

        response.flip();
        // 大批量响应（可达 maxBytes 级别）单次 write 可能只写出一部分，必须循环写
        writeFully(clientChannel, response);
    }

    /** 循环写，直到 buffer 全部写到对端，避免大响应被截断。 */
    private static void writeFully(SocketChannel channel, ByteBuffer buffer) throws IOException {
        while (buffer.hasRemaining()) {
            int written = channel.write(buffer);
            if (written < 0) {
                throw new IOException("Connection closed while writing response");
            }
            if (written == 0) {
                // 被接受的连接是非阻塞的：写不进去要让出 CPU，不能忙等
                try {
                    Thread.sleep(1L);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Interrupted while writing response", e);
                }
            }
        }
    }

    /**
     * Handle metadata request from client
     */
    private void handleMetadataRequest(SocketChannel clientChannel, ByteBuffer buffer) throws IOException {
        // Prepare response with metadata
        int size = 5; // 1 byte for response type, 4 bytes for topic count

        // Calculate size for topics metadata
        for (Map.Entry<String, List<Partition>> entry : topics.entrySet()) {
            size += 6 + entry.getKey().length(); // 2 bytes for length, string, 4 bytes for partition count

            // Add size for each partition
            size += entry.getValue().size() * 12; // 4 bytes for id, 4 bytes for leader, 4 bytes for follower count

            // Add size for followers
            for (Partition partition : entry.getValue()) {
                size += partition.getFollowers().size() * 4; // 4 bytes per follower ID
            }
        }

        // Add size for brokers metadata
        size += 4; // 4 bytes for broker count
        size += clusterMetadata.size() * 10; // 4 bytes for id, 2 bytes for host length, 4 bytes for port

        // Add estimated size for broker hostnames
        for (BrokerInfo broker : clusterMetadata.values()) {
            size += broker.getHost().length();
        }

        ByteBuffer response = ByteBuffer.allocate(size);
        response.put(Protocol.METADATA_RESPONSE);

        // Add broker metadata
        response.putInt(clusterMetadata.size());
        for (BrokerInfo broker : clusterMetadata.values()) {
            response.putInt(broker.getId());
            response.putShort((short) broker.getHost().length());
            response.put(broker.getHost().getBytes());
            response.putInt(broker.getPort());
        }

        // Add topic metadata
        response.putInt(topics.size());
        for (Map.Entry<String, List<Partition>> entry : topics.entrySet()) {
            String topic = entry.getKey();
            List<Partition> partitions = entry.getValue();

            response.putShort((short) topic.length());
            response.put(topic.getBytes());
            response.putInt(partitions.size());

            for (Partition partition : partitions) {
                response.putInt(partition.getId());
                response.putInt(partition.getLeader());

                List<Integer> followers = partition.getFollowers();
                response.putInt(followers.size());
                for (Integer follower : followers) {
                    response.putInt(follower);
                }
            }
        }

        response.flip();
        clientChannel.write(response);
    }

    /**
     * Handle create topic request from client
     */
    private void handleCreateTopicRequest(SocketChannel clientChannel, ByteBuffer buffer) throws IOException {
        short topicLength = buffer.getShort();
        byte[] topicBytes = new byte[topicLength];
        buffer.get(topicBytes);
        String topic = new String(topicBytes);

        int numPartitions = buffer.getInt();
        short replicationFactor = buffer.getShort();

        LOGGER.info("Create topic request: " + topic +
                ", partitions: " + numPartitions +
                ", replication: " + replicationFactor);

        // Check if topic already exists
        if (topics.containsKey(topic)) {
            Protocol.sendErrorResponse(clientChannel, "Topic already exists");
            return;
        }

        // Validate parameters
        if (numPartitions <= 0 || replicationFactor <= 0 ||
                replicationFactor > clusterMetadata.size()) {
            Protocol.sendErrorResponse(clientChannel, "Invalid topic configuration");
            return;
        }

        // As controller, create the topic
        if (amController()) {
            createTopicLocally(clientChannel, topic, numPartitions, replicationFactor);
        } else {
            // Forward to controller
            forwardCreateTopicToController(clientChannel, topic, numPartitions, replicationFactor);
        }
    }

    /**
     * Forward create topic request to controller
     */
    private void forwardCreateTopicToController(SocketChannel clientChannel, String topic,
            int numPartitions, short replicationFactor) throws IOException {
        // Find controller
        int controllerId = -1;
        try {
            String controllerData = zkClient.getData("/controller");
            controllerId = Integer.parseInt(controllerData.trim());
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to get controller info", e);
            Protocol.sendErrorResponse(clientChannel, "Controller not available");
            return;
        }

        BrokerInfo controller = clusterMetadata.get(controllerId);
        if (controller == null) {
            Protocol.sendErrorResponse(clientChannel, "Controller broker not available");
            return;
        }

        // 防自环：ZooKeeper 里的 controller 就是本 broker（很常见：/controller 节点是自己，
        // 但内存里的 isController 标记尚未跟上来）。此时必须本地处理。
        // 否则请求会在自己身上反复转发，把工作线程全部占满，最终整个 broker 不再响应任何请求。
        if (controllerId == brokerId || controller.getPort() == brokerPort) {
            LOGGER.warning("Forward target is this broker (id=" + controllerId + ", "
                    + controller.getHost() + ":" + controller.getPort()
                    + "); creating topic locally instead of self-forwarding");
            createTopicLocally(clientChannel, topic, numPartitions, replicationFactor);
            return;
        }

        // Prepare forwarded create topic request
        ByteBuffer request = ByteBuffer.allocate(9 + topic.length());
        request.put(Protocol.CREATE_TOPIC);
        request.putShort((short) topic.length());
        request.put(topic.getBytes(StandardCharsets.UTF_8));
        request.putInt(numPartitions);
        request.putShort(replicationFactor);
        request.flip();
        byte[] payload = request.array();

        LOGGER.info("Forwarding create topic '" + topic + "' to controller id=" + controllerId
                + " (" + controller.getHost() + ":" + controller.getPort() + ")");

        // 用普通 Socket 而不是 SocketChannel：SocketChannel 不遵守 SO_TIMEOUT，
        // 对端不响应时会永久阻塞。
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(controller.getHost(), controller.getPort()),
                    CONTROL_CONNECT_TIMEOUT_MS);
            socket.setSoTimeout(CONTROL_READ_TIMEOUT_MS);
            socket.getOutputStream().write(payload);
            socket.getOutputStream().flush();

            byte[] reply = new byte[2];
            InputStream in = socket.getInputStream();
            int total = 0;
            while (total < reply.length) {
                int read = in.read(reply, total, reply.length - total);
                if (read < 0) {
                    break;
                }
                total += read;
            }
            if (total < reply.length) {
                throw new IOException("controller closed the connection without a response");
            }

            // Forward controller's response back to client
            writeFully(clientChannel, ByteBuffer.wrap(reply, 0, total));
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Failed to forward create topic request to controller", e);
            Protocol.sendErrorResponse(clientChannel, "Failed to forward to controller: " + e.getMessage());
        }
    }

    /**
     * 以 controller 身份在本地创建 topic 并把结果回给客户端。
     */
    private void createTopicLocally(SocketChannel clientChannel, String topic, int numPartitions,
            short replicationFactor) {
        isController.set(true);
        createTopic(topic, numPartitions, replicationFactor);

        ByteBuffer response = ByteBuffer.allocate(2);
        response.put(Protocol.CREATE_TOPIC_RESPONSE);
        response.put((byte) 0); // 0 = success
        response.flip();
        try {
            writeFully(clientChannel, response);
        } catch (IOException e) {
            LOGGER.log(Level.WARNING, "Failed to send create topic response to client", e);
        }
    }

    /**
     * Create a new topic with the specified configuration
     */
    private void createTopic(String topic, int numPartitions, short replicationFactor) {
        if (!isController.get()) {
            LOGGER.warning("Only the controller can create topics");
            return;
        }

        try {
            // Create topic directory
            String topicDir = DATA_DIR + File.separator + brokerId + File.separator + topic;
            new File(topicDir).mkdirs();

            // Create topic in ZooKeeper
            String topicPath = "/topics/" + topic;
            if (!zkClient.exists(topicPath)) {
                zkClient.createPersistentNode(topicPath, "");
                zkClient.createPersistentNode(topicPath + "/partitions", "");
            }

            // Create partitions
            List<Partition> partitions = new ArrayList<>();
            // 以 ZooKeeper 中的实时注册为准，避免把已下线的节点选为 leader/follower 导致复制请求悬空
            List<Integer> brokerIds = new ArrayList<>();
            try {
                for (String id : zkClient.getChildren("/brokers")) {
                    brokerIds.add(Integer.parseInt(id));
                }
            } catch (Exception e) {
                LOGGER.log(Level.WARNING, "Failed to read live brokers from ZooKeeper, using cached metadata", e);
            }
            if (brokerIds.isEmpty()) {
                brokerIds.addAll(clusterMetadata.keySet());
            }
            LOGGER.info("Creating topic " + topic + " over live brokers " + brokerIds);

            for (int i = 0; i < numPartitions; i++) {
                int partitionId = i;
                String partitionDir = topicDir + File.separator + partitionId;
                new File(partitionDir).mkdirs();

                // Select leader and followers
                int leaderIndex = i % brokerIds.size();
                int leaderId = brokerIds.get(leaderIndex);

                List<Integer> followers = new ArrayList<>();
                for (int j = 1; j < replicationFactor; j++) {
                    int followerIndex = (leaderIndex + j) % brokerIds.size();
                    followers.add(brokerIds.get(followerIndex));
                }

                // Create partition
                Partition partition = new Partition(partitionId, leaderId, followers, partitionDir);
                partitions.add(partition);

                // Store partition metadata in ZooKeeper
                String partitionPath = topicPath + "/partitions/" + partitionId;
                String partitionData = leaderId + ";";
                for (int follower : followers) {
                    partitionData += follower + ",";
                }

                zkClient.createPersistentNode(partitionPath, partitionData);

                LOGGER.info("Created partition " + partitionId +
                        " for topic " + topic +
                        " with leader " + leaderId +
                        " and followers " + followers);
            }

            // Add topic to broker's metadata
            topics.put(topic, partitions);

            // Notify all brokers to load the topic
            for (int brokerId : brokerIds) {
                if (brokerId != this.brokerId) {
                    notifyBrokerForTopicCreation(brokerId, topic);
                }
            }
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to create topic", e);
        }
    }
}
