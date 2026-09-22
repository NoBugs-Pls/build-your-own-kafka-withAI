// 整个文件替换：支持任意数量 broker 的自由增减，并自动接管已运行的外部 broker 进程
package com.simplekafka.dashboard;

import com.simplekafka.broker.ZookeeperClient;
import com.simplekafka.client.SimpleKafkaClient;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import org.apache.zookeeper.KeeperException;

/**
 * 浏览器可视化控制台：集中操控 ZooKeeper 中心、Broker、生产者和消费者。
 *
 * <p>节点数量不设上限：既可以一次启动 N 个 broker，也可以随时「增加节点」/「移除节点」。
 * 对于不是由本控制台启动、但已在运行的 broker，控制台会通过进程句柄自动接管，
 * 因此刷新 dashboard 之后依然可以重启、强杀或移除它们。
 *
 * <ul>
 *   <li>页面：{@code /} 与 {@code /control} 是独立控制台；{@code /dashboard} 保留旧页面。</li>
 *   <li>集群：{@code /api/cluster/start}（count/basePort/zkPort）、{@code /api/cluster/add}、
 *       {@code /api/cluster/remove}、{@code /api/cluster/stop}。</li>
 *   <li>进程：{@code /api/process}（restart/stop/kill）。</li>
 *   <li>消息：{@code /api/topic}、{@code /api/produce}、{@code /api/fetch}。</li>
 *   <li>ZooKeeper 中心：{@code /api/zk/tree|read|create|set|delete}。</li>
 *   <li>观测：{@code /api/state}、{@code /api/events}。</li>
 * </ul>
 */
public final class DashboardServer {
  private static final int DEFAULT_PORT = 8080;
  private static final int DEFAULT_BASE_PORT = 9091;
  private static final Path CONTROL_ROOM = Paths.get("src", "main", "resources", "controlroom", "index.html");
  private static final Path LEGACY_UI = Paths.get("src", "main", "resources", "dashboard", "index.html");
  private static final Path DATA_ROOT = Paths.get("data");
  /** java.util.logging 的头部行，例如 {@code Sep 22, 2026 3:54:18 PM com.example.Foo bar}。 */
  private static final java.util.regex.Pattern JUL_HEADER =
      java.util.regex.Pattern.compile("[A-Z][a-z]{2} \\d{1,2}, \\d{4},? \\d{1,2}:\\d{2}:\\d{2} [AP]M .*");
  private static final java.util.regex.Pattern JUL_LEVEL =
      java.util.regex.Pattern.compile("(SEVERE|WARNING|INFO|CONFIG|FINE|FINER|FINEST|ALL|OFF):?");

  /**
   * 判断是否是「噪声行」，用于让日志面板只保留可读的关键信息：
   *
   * <ul>
   *   <li>三方库的 DEBUG 日志（ZooKeeper 客户端等）；</li>
   *   <li>java.util.logging 的多行头部（日期行、类名/方法名行、单独的级别词）；</li>
   *   <li>异常堆栈的 {@code at ...} 帧（异常首行仍会保留，便于看出原因）；</li>
   *   <li>只有标点的续行（多行日志产生的 {@code ]} 之类）。</li>
   * </ul>
   */
  private static boolean isNoisyLogLine(String line) {
    if (line.contains(" DEBUG ") || line.contains("Got ping response")) return true;
    if (line.startsWith("    at ") || line.startsWith("\tat ")) return true;
    if (JUL_HEADER.matcher(line).matches()) return true;
    if (JUL_LEVEL.matcher(line).matches()) return true;
    // 形如 com.example.Foo.method 的类名/方法名行：没有空格且含包名点号
    if (line.indexOf(' ') < 0 && line.matches("[\\w$]+(\\.[\\w$]+)+")) return true;
    if (line.replaceAll("[\\[\\]{}(),;:'\"\\s]", "").isEmpty()) return true;
    return false;
  }

  private final Map<String, ManagedProcess> processes = new ConcurrentHashMap<>();
  private final List<String> eventLog = new CopyOnWriteArrayList<>();
  private final AtomicLong eventSequence = new AtomicLong();
  private final HttpServer server;
  private final int zkPort;

  private DashboardServer(int port, int zkPort) throws IOException {
    this.zkPort = zkPort;
    this.server = HttpServer.create(new InetSocketAddress("0.0.0.0", port), 0);
    server.setExecutor(Executors.newCachedThreadPool());
    server.createContext("/", exchange -> servePage(exchange, CONTROL_ROOM));
    server.createContext("/control", exchange -> servePage(exchange, CONTROL_ROOM));
    server.createContext("/dashboard", exchange -> servePage(exchange, LEGACY_UI));
    server.createContext("/api/state", this::state);
    server.createContext("/api/events", this::events);
    server.createContext("/api/cluster/start", this::startCluster);
    server.createContext("/api/cluster/add", this::addBroker);
    server.createContext("/api/cluster/remove", this::removeBroker);
    server.createContext("/api/cluster/stop", this::stopCluster);
    server.createContext("/api/process", this::processAction);
    server.createContext("/api/produce", this::produce);
    server.createContext("/api/fetch", this::fetch);
    server.createContext("/api/topic", this::createTopic);
    server.createContext("/api/metadata", this::brokerMetadata);
    server.createContext("/api/zk/tree", this::zkTree);
    server.createContext("/api/zk/read", this::zkRead);
    server.createContext("/api/zk/create", this::zkCreate);
    server.createContext("/api/zk/set", this::zkSet);
    server.createContext("/api/zk/delete", this::zkDelete);
  }

  public static void main(String[] args) throws Exception {
    int port = args.length > 0 ? Integer.parseInt(args[0]) : DEFAULT_PORT;
    int zkPort = args.length > 1 ? Integer.parseInt(args[1]) : 2181;
    DashboardServer dashboard = new DashboardServer(port, zkPort);
    dashboard.server.start();
    dashboard.log("Dashboard listening on http://localhost:" + port);
    dashboard.log("Control room ready at http://localhost:" + port + "/control");
  }

  private static void servePage(HttpExchange exchange, Path file) throws IOException {
    if (!"GET".equals(exchange.getRequestMethod())) {
      send(exchange, 405, "text/plain", "Method Not Allowed");
      return;
    }
    if (!Files.exists(file)) {
      send(exchange, 404, "text/plain", "Dashboard UI is not packaged yet: " + file);
      return;
    }
    send(exchange, 200, "text/html; charset=utf-8", Files.readString(file));
  }

  private void state(HttpExchange exchange) throws IOException {
    if (!requireGet(exchange)) return;
    Map<String, String> query = query(exchange.getRequestURI());
    int requestedZkPort = intValue(query, "zkPort", zkPort);
    // 一次连接同时拿到元数据 JSON 与 broker 注册表，避免每次刷新重复连接 ZooKeeper
    Map<Integer, String> registry = new HashMap<>();
    String zkJson = zookeeperJson(requestedZkPort, registry);
    adoptExternalBrokers(requestedZkPort, registry);
    StringBuilder json = new StringBuilder("{\"processes\":[");
    List<ManagedProcess> managed = new ArrayList<>(processes.values());
    managed.sort(Comparator.comparing(ManagedProcess::name));
    for (int i = 0; i < managed.size(); i++) {
      if (i > 0) json.append(',');
      json.append(managed.get(i).json());
    }
    json.append("],\"zookeeper\":").append(zkJson);
    json.append(",\"data\":").append(dataJson());
    json.append("}");
    sendJson(exchange, 200, json.toString());
  }

  /** 读取 ZooKeeper 中心状态：controller、broker 注册、topic/分区元数据。 */
  private String zookeeperJson(int port, Map<Integer, String> registryOut) {
    ZookeeperClient client = new ZookeeperClient("localhost", port);
    try {
      client.connect();
      List<String> brokerIds = client.exists("/brokers")
          ? client.getChildren("/brokers") : Collections.emptyList();
      List<String> topics = client.exists("/topics")
          ? client.getChildren("/topics") : Collections.emptyList();
      String controller = client.exists("/controller")
          ? client.getData("/controller") : "";
      StringBuilder json = new StringBuilder("{\"connected\":true,\"controller\":");
      json.append(quote(controller)).append(",\"brokers\":[");
      for (int i = 0; i < brokerIds.size(); i++) {
        if (i > 0) json.append(',');
        String id = brokerIds.get(i);
        String address = client.getData("/brokers/" + id);
        json.append("{\"id\":").append(id).append(",\"address\":").append(quote(address)).append('}');
        if (registryOut != null) {
          int brokerId = parseIntSafe(id, -1);
          if (brokerId > 0) registryOut.put(brokerId, address);
        }
      }
      json.append("],\"topics\":[");
      for (int i = 0; i < topics.size(); i++) {
        if (i > 0) json.append(',');
        String topic = topics.get(i);
        json.append("{\"name\":").append(quote(topic)).append(",\"partitions\":[");
        String path = "/topics/" + topic + "/partitions";
        List<String> partitions = client.exists(path) ? client.getChildren(path) : Collections.emptyList();
        for (int j = 0; j < partitions.size(); j++) {
          if (j > 0) json.append(',');
          String partition = partitions.get(j);
          String raw = client.getData(path + "/" + partition);
          json.append("{\"id\":").append(partition).append(",\"assignment\":")
              .append(quote(formatAssignment(raw))).append('}');
        }
        json.append("]}");
      }
      json.append("]}");
      client.close();
      return json.toString();
    } catch (Exception e) {
      try { client.close(); } catch (Exception ignored) { }
      return "{\"connected\":false,\"error\":" + quote(message(e)) + "}";
    }
  }

  /** 分区元数据在 ZooKeeper 中保存为 {@code leader;follower1,follower2,...}，这里格式化为可读文本。 */
  private static String formatAssignment(String raw) {
    if (raw == null) return "";
    String[] parts = raw.split(";");
    String leader = parts.length > 0 ? parts[0] : "";
    String followers = parts.length > 1 ? parts[1] : "";
    return "leader=" + leader + ", followers=[" + followers + "]";
  }

  private String dataJson() {
    try {
      if (!Files.exists(DATA_ROOT)) return "[]";
      List<String> entries = new ArrayList<>();
      try (Stream<Path> stream = Files.walk(DATA_ROOT)) {
        stream.filter(Files::isRegularFile).forEach(path -> {
          try { entries.add("{\"path\":" + quote(path.toString()) + ",\"bytes\":"
              + Files.size(path) + "}"); } catch (IOException ignored) { }
        });
      }
      return "[" + String.join(",", entries) + "]";
    } catch (IOException e) {
      return "[]";
    }
  }

  private void events(HttpExchange exchange) throws IOException {
    if (!requireGet(exchange)) return;
    StringBuilder json = new StringBuilder("[");
    int start = Math.max(0, eventLog.size() - 200);
    for (int i = start; i < eventLog.size(); i++) {
      if (i > start) json.append(',');
      json.append(eventLog.get(i));
    }
    sendJson(exchange, 200, json.append(']').toString());
  }

  // ============================== 集群启停与节点增减 ==============================

  /** 一次性启动 1..count 个 broker；已在运行的节点会被复用，不会因为端口占用而失败。 */
  private void startCluster(HttpExchange exchange) throws IOException {
    if (!requirePost(exchange)) return;
    Map<String, String> body = body(exchange);
    int count = intValue(body, "count", 3);
    int basePort = intValue(body, "basePort", DEFAULT_BASE_PORT);
    int requestedZkPort = intValue(body, "zkPort", this.zkPort);
    startZooKeeper(requestedZkPort);
    waitForPort(requestedZkPort, 20_000L);
    for (int id = 1; id <= count; id++) {
      startBroker(id, basePort + id - 1, requestedZkPort);
    }
    log("Cluster start requested: " + count + " node(s), base port " + basePort);
    sendJson(exchange, 200, "{\"ok\":true,\"count\":" + count + "}");
  }

  /**
   * 新增一个 broker：`id` 与 `port` 都可以省略，省略时自动分配。
   *
   * <p>启动前会做三项检查：id 是否已有存活进程、端口是否被占用、
   * 以及 ZooKeeper 里是否残留了同 id 的注册节点（例如该节点被强杀后旧会话尚未过期）。
   * 残留节点会被主动清理，否则新 broker 会因为路径被占用而无法注册。
   */
  private void addBroker(HttpExchange exchange) throws IOException {
    if (!requirePost(exchange)) return;
    Map<String, String> body = body(exchange);
    int requestedZkPort = intValue(body, "zkPort", this.zkPort);
    int basePort = intValue(body, "basePort", DEFAULT_BASE_PORT);
    startZooKeeper(requestedZkPort);
    waitForPort(requestedZkPort, 20_000L);
    adoptExternalBrokers(requestedZkPort);

    int id = body.containsKey("id") && !body.get("id").isBlank()
        ? intValue(body, "id", 0) : nextBrokerId(requestedZkPort);
    int port = body.containsKey("port") && !body.get("port").isBlank()
        ? intValue(body, "port", 0) : basePort + id - 1;
    if (id <= 0 || port < 1024 || port > 65535) {
      sendJson(exchange, 400, "{\"ok\":false,\"error\":\"invalid broker id or port\"}");
      return;
    }

    ManagedProcess existing = processes.get("broker-" + id);
    if (existing != null && existing.isAlive()) {
      sendJson(exchange, 400, "{\"ok\":false,\"error\":\"broker-" + id + " is already running\"}");
      return;
    }
    if (!portAvailable(port)) {
      sendJson(exchange, 400, "{\"ok\":false,\"error\":\"port " + port + " is already in use\"}");
      return;
    }

    clearStaleRegistration(id, requestedZkPort);
    startBroker(id, port, requestedZkPort);
    log("Added broker-" + id + " on port " + port);
    sendJson(exchange, 200, "{\"ok\":true,\"id\":" + id + ",\"port\":" + port + "}");
  }

  /**
   * 清理残留的注册节点。
   *
   * <p>若 ZooKeeper 中已存在 {@code /brokers/<id>}，但既没有控制台托管的进程，
   * 也找不到任何存活的外部进程与之对应（典型场景：节点被强杀后会话还没过期），
   * 就删除该节点，让马上要启动的同 id broker 可以正常注册。
   *
   * @return true 表示该 id 已可安全使用
   */
  private boolean clearStaleRegistration(int id, int port) {
    ManagedProcess managed = processes.get("broker-" + id);
    if (managed != null && managed.isAlive()) return false;

    Map<Integer, String> registered = registeredBrokers(port);
    String address = registered.get(id);
    if (address == null) return true;

    String[] parts = address.split(":", 2);
    int registeredPort = -1;
    if (parts.length == 2) {
      try {
        registeredPort = Integer.parseInt(parts[1]);
      } catch (NumberFormatException ignored) {
        // 数据异常时按残留节点处理
      }
    }
    if (registeredPort > 0 && findBrokerProcess(id, registeredPort) != null) {
      return false;
    }

    ZookeeperClient client = new ZookeeperClient("localhost", port);
    try {
      client.connect();
      client.deleteNode("/brokers/" + id);
      log("cleared stale registry node /brokers/" + id);
      return true;
    } catch (Exception e) {
      log("failed to clear stale registry node /brokers/" + id + ": " + message(e));
      return false;
    } finally {
      try { client.close(); } catch (Exception ignored) { }
    }
  }

  /** 移除一个 broker：优雅停机，触发 shutdown hook 关闭 ZooKeeper 会话，临时节点随即消失。 */
  private void removeBroker(HttpExchange exchange) throws IOException {
    if (!requirePost(exchange)) return;
    Map<String, String> body = body(exchange);
    int id = intValue(body, "id", -1);
    int requestedZkPort = intValue(body, "zkPort", this.zkPort);
    if (id <= 0) {
      sendJson(exchange, 400, "{\"ok\":false,\"error\":\"invalid broker id\"}");
      return;
    }
    String name = "broker-" + id;
    ManagedProcess process = processes.get(name);
    if (process == null || !process.isAlive()) {
      adoptExternalBrokers(requestedZkPort);
      process = processes.get(name);
    }
    if (process == null || !process.isAlive()) {
      sendJson(exchange, 400, "{\"ok\":false,\"error\":\"broker-" + id + " is not running or not reachable\"}");
      return;
    }
    process.stop(false);
    boolean deregistered = waitForDeregistration(id, requestedZkPort, 10_000L);
    processes.remove(name);
    log("Removed broker-" + id + (deregistered ? "; registry node cleared" : "; registry node still present"));
    sendJson(exchange, 200, "{\"ok\":true,\"deregistered\":" + deregistered + "}");
  }

  /**
   * 停止全部：先接管已知进程，再逐个优雅停止，超时自动升级为强杀；
   * 最后做一次兜底清扫，确保不留下僵留的 broker / ZooKeeper 进程。
   */
  private void stopCluster(HttpExchange exchange) throws IOException {
    if (!requirePost(exchange)) return;
    Map<String, String> body = body(exchange);
    int requestedZkPort = intValue(body, "zkPort", this.zkPort);
    adoptExternalBrokers(requestedZkPort);

    int stopped = 0;
    int forced = 0;
    for (ManagedProcess process : new ArrayList<>(processes.values())) {
      if (!process.isAlive()) continue;
      if (process.stop(false)) forced++;
      stopped++;
    }
    processes.clear();

    // 兜底清扫：仍有存活的集群进程说明没有响应优雅信号，直接强制结束
    int swept = 0;
    for (BrokerProcess broker : scanBrokerProcesses()) {
      broker.handle.destroyForcibly();
      swept++;
    }
    for (ProcessHandle handle : scanZooKeeperProcesses(requestedZkPort)) {
      handle.destroyForcibly();
      swept++;
    }

    log("Cluster stopped: " + stopped + " managed, " + forced + " escalated, " + swept + " force-swept");
    sendJson(exchange, 200, "{\"ok\":true,\"stopped\":" + stopped + ",\"forced\":" + forced
        + ",\"swept\":" + swept + "}");
  }

  private void processAction(HttpExchange exchange) throws IOException {
    if (!requirePost(exchange)) return;
    Map<String, String> body = body(exchange);
    String action = body.get("action");
    String name = body.get("name");
    adoptExternalBrokers(intValue(body, "zkPort", this.zkPort));
    ManagedProcess process = processes.get(name);
    if (process == null) {
      sendJson(exchange, 404, "{\"ok\":false,\"error\":\"unknown process\"}");
      return;
    }
    if ("kill".equals(action)) process.stop(true);
    else if ("stop".equals(action)) process.stop(false);
    else if ("restart".equals(action)) { process.stop(true); process.restart(); }
    else { sendJson(exchange, 400, "{\"ok\":false,\"error\":\"unknown action\"}"); return; }
    sendJson(exchange, 200, "{\"ok\":true}");
  }

  /** 自动挑选下一个可用的 broker id：托管进程与 ZooKeeper 注册节点的最大 id + 1。 */
  private int nextBrokerId(int requestedZkPort) {
    int max = 0;
    for (String name : processes.keySet()) {
      if (name.startsWith("broker-")) {
        try {
          max = Math.max(max, Integer.parseInt(name.substring("broker-".length())));
        } catch (NumberFormatException ignored) {
          // 名称不符合 broker-<id> 时忽略
        }
      }
    }
    for (Integer id : registeredBrokers(requestedZkPort).keySet()) {
      max = Math.max(max, id);
    }
    return max + 1;
  }

  /** 读取 ZooKeeper 中注册的 broker（id -> host:port）；ZooKeeper 不可用时返回空表。 */
  private Map<Integer, String> registeredBrokers(int port) {
    Map<Integer, String> result = new HashMap<>();
    ZookeeperClient client = new ZookeeperClient("localhost", port);
    try {
      client.connect();
      if (!client.exists("/brokers")) return result;
      for (String child : client.getChildren("/brokers")) {
        try {
          result.put(Integer.parseInt(child), client.getData("/brokers/" + child));
        } catch (NumberFormatException ignored) {
          // 跳过非 broker id 的节点
        }
      }
    } catch (Exception e) {
      // ZooKeeper 未就绪时静默返回，页面会显示“不可用”
    } finally {
      try { client.close(); } catch (Exception ignored) { }
    }
    return result;
  }

  /**
   * 接管已在运行、但不是由本控制台启动的进程（ZooKeeper 与所有 broker）。
   *
   * <p>这样即使 dashboard 被重启，或者用命令行手动启动了额外节点，页面依然能显示它们，
   * 并且可以执行重启 / 强杀 / 移除 / 停止全部。
   *
   * <p>若同一个 broker id 存在多个进程（例如旧的进程卡在停机流程里没有退出），
   * 会保留最新启动的那个（它是真正持有端口的进程），其余按僵留进程强制清理。
   */
  private void adoptExternalBrokers(int port) {
    adoptExternalBrokers(port, registeredBrokers(port));
  }

  private void adoptExternalBrokers(int port, Map<Integer, String> registered) {
    adoptZooKeeper(port);
    Map<Integer, List<BrokerProcess>> byId = new LinkedHashMap<>();
    for (BrokerProcess broker : scanBrokerProcesses()) {
      byId.computeIfAbsent(broker.id, ignored -> new ArrayList<>()).add(broker);
    }

    for (Map.Entry<Integer, List<BrokerProcess>> entry : byId.entrySet()) {
      int id = entry.getKey();
      String name = "broker-" + id;
      List<BrokerProcess> candidates = entry.getValue();

      // 同一 id 有多个进程时，保留最新启动的（旧进程通常已经释放端口但卡在停机流程）
      BrokerProcess keep = candidates.get(0);
      for (BrokerProcess candidate : candidates) {
        if (startTime(candidate.handle) > startTime(keep.handle)) keep = candidate;
      }
      for (BrokerProcess candidate : candidates) {
        if (candidate.handle.pid() != keep.handle.pid()) {
          candidate.handle.destroyForcibly();
          log("killed duplicate " + name + " process (pid " + candidate.handle.pid() + ")");
        }
      }

      int brokerPort = portOf(registered.getOrDefault(id, ""));
      if (brokerPort <= 0) brokerPort = keep.port;

      ManagedProcess existing = processes.get(name);
      if (existing != null && existing.isAlive() && existing.pid() == keep.handle.pid()) continue;

      ManagedProcess adopted = new ManagedProcess(name,
          List.of("com.simplekafka.broker.SimpleKafkaBroker", String.valueOf(id), "localhost",
              String.valueOf(brokerPort), String.valueOf(port)),
          System.getProperty("java.class.path"), id, brokerPort, port);
      adopted.handle = keep.handle;
      processes.put(name, adopted);
      log("adopted running " + name + " (pid " + keep.handle.pid() + ")");
    }

    // 清理已退出且不在 ZooKeeper 注册表中的 broker 记录
    for (Map.Entry<String, ManagedProcess> entry : new ArrayList<>(processes.entrySet())) {
      ManagedProcess process = entry.getValue();
      if (process.isBroker() && !process.isAlive() && !registered.containsKey(process.brokerId)) {
        processes.remove(entry.getKey());
      }
    }
  }

  /** 接管 ZooKeeper 服务进程：通过配置文件的 clientPort 与本控制台使用的端口比对确认身份。 */
  private void adoptZooKeeper(int port) {
    ManagedProcess existing = processes.get("zookeeper");
    if (existing != null && existing.isAlive()) return;
    for (ProcessHandle handle : scanZooKeeperProcesses(port)) {
      ManagedProcess adopted = new ManagedProcess("zookeeper", List.of(),
          System.getProperty("java.class.path"), -1, -1, port);
      adopted.handle = handle;
      processes.put("zookeeper", adopted);
      log("adopted running zookeeper (pid " + handle.pid() + ")");
      return;
    }
  }

  private static long startTime(ProcessHandle handle) {
    return handle.info().startInstant().map(java.time.Instant::toEpochMilli).orElse(handle.pid());
  }

  private static int portOf(String address) {
    if (address == null) return -1;
    int index = address.lastIndexOf(':');
    if (index < 0 || index == address.length() - 1) return -1;
    return parseIntSafe(address.substring(index + 1).trim(), -1);
  }

  private static int parseIntSafe(String text, int fallback) {
    try {
      return Integer.parseInt(text.trim());
    } catch (Exception e) {
      return fallback;
    }
  }

  /** 扫描所有 {@code SimpleKafkaBroker <id> <host> <port> <zkPort>} 进程。 */
  private static List<BrokerProcess> scanBrokerProcesses() {
    List<BrokerProcess> found = new ArrayList<>();
    try (Stream<ProcessHandle> handles = ProcessHandle.allProcesses()) {
      for (ProcessHandle handle : handles.toArray(ProcessHandle[]::new)) {
        if (!handle.isAlive()) continue;
        Optional<String> line = handle.info().commandLine();
        if (line.isEmpty()) continue;
        String[] parts = line.get().split("\\s+");
        for (int i = 0; i + 3 < parts.length; i++) {
          if (parts[i].endsWith("SimpleKafkaBroker")) {
            int id = parseIntSafe(parts[i + 1], -1);
            int port = parseIntSafe(parts[i + 3], -1);
            if (id > 0) found.add(new BrokerProcess(id, port, handle));
            break;
          }
        }
      }
    } catch (Exception ignored) {
      // 进程扫描失败时按“没有外部进程”处理
    }
    return found;
  }

  /** 扫描监听指定端口的 ZooKeeper 服务进程。 */
  private static List<ProcessHandle> scanZooKeeperProcesses(int zkPort) {
    List<ProcessHandle> result = new ArrayList<>();
    try (Stream<ProcessHandle> handles = ProcessHandle.allProcesses()) {
      for (ProcessHandle handle : handles.toArray(ProcessHandle[]::new)) {
        if (!handle.isAlive()) continue;
        Optional<String> line = handle.info().commandLine();
        if (line.isEmpty()) continue;
        String[] parts = line.get().split("\\s+");
        for (int i = 0; i + 1 < parts.length; i++) {
          if (parts[i].endsWith("QuorumPeerMain")) {
            if (configClientPort(parts[i + 1]) == zkPort) result.add(handle);
            break;
          }
        }
      }
    } catch (Exception ignored) {
      // 忽略扫描异常
    }
    return result;
  }

  private static int configClientPort(String configPath) {
    try {
      for (String line : Files.readAllLines(Paths.get(configPath))) {
        String trimmed = line.trim();
        if (trimmed.startsWith("clientPort=")) {
          return parseIntSafe(trimmed.substring("clientPort=".length()), -1);
        }
      }
    } catch (Exception ignored) {
      // 配置不可读时无法确认身份
    }
    return -1;
  }

  /** 通过进程命令行匹配 {@code SimpleKafkaBroker <id> ...}，找到对应进程句柄。 */
  private static ProcessHandle findBrokerProcess(int brokerId, int port) {
    for (BrokerProcess broker : scanBrokerProcesses()) {
      if (broker.id == brokerId && (port <= 0 || broker.port == port)) {
        return broker.handle;
      }
    }
    return null;
  }

  /** broker 进程的扫描结果。 */
  private static final class BrokerProcess {
    private final int id;
    private final int port;
    private final ProcessHandle handle;

    private BrokerProcess(int id, int port, ProcessHandle handle) {
      this.id = id;
      this.port = port;
      this.handle = handle;
    }
  }

  /** 等待 ZooKeeper 中的临时注册节点消失，表示该 broker 已真正下线。 */
  private boolean waitForDeregistration(int id, int port, long timeoutMs) {
    long deadline = System.currentTimeMillis() + timeoutMs;
    while (System.currentTimeMillis() < deadline) {
      ZookeeperClient client = new ZookeeperClient("localhost", port);
      try {
        client.connect();
        if (!client.exists("/brokers/" + id)) return true;
      } catch (Exception ignored) {
        // ZooKeeper 暂时不可用时继续轮询
      } finally {
        try { client.close(); } catch (Exception ignored) { }
      }
      try {
        Thread.sleep(400L);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return false;
      }
    }
    return false;
  }

  // ============================== 消息操作 ==============================

  private void produce(HttpExchange exchange) throws IOException {
    if (!requirePost(exchange)) return;
    Map<String, String> body = body(exchange);
    String topic = body.get("topic");
    String message = body.get("message");
    int partition = intValue(body, "partition", 0);
    int port = intValue(body, "port", DEFAULT_BASE_PORT);
    try {
      SimpleKafkaClient client = new SimpleKafkaClient("localhost", port);
      client.initialize();
      long offset = client.send(topic, partition, message.getBytes(StandardCharsets.UTF_8));
      log("Produced topic=" + topic + " partition=" + partition + " offset=" + offset);
      sendJson(exchange, 200, "{\"ok\":true,\"offset\":" + offset + "}");
    } catch (Exception e) {
      log("Produce failed topic=" + topic + " partition=" + partition + ": " + message(e));
      sendJson(exchange, 400, "{\"ok\":false,\"error\":" + quote(message(e)) + "}");
    }
  }

  private void fetch(HttpExchange exchange) throws IOException {
    if (!requirePost(exchange)) return;
    Map<String, String> body = body(exchange);
    String topic = body.get("topic");
    int partition = intValue(body, "partition", 0);
    long offset = longValue(body, "offset", 0L);
    int port = intValue(body, "port", DEFAULT_BASE_PORT);
    try {
      SimpleKafkaClient client = new SimpleKafkaClient("localhost", port);
      client.initialize();
      List<byte[]> messages = client.fetch(topic, partition, offset, 1024 * 1024);
      StringBuilder json = new StringBuilder("{\"ok\":true,\"messages\":[");
      for (int i = 0; i < messages.size(); i++) {
        if (i > 0) json.append(',');
        json.append("{\"offset\":").append(offset + i).append(",\"message\":")
            .append(quote(new String(messages.get(i), StandardCharsets.UTF_8))).append('}');
      }
      sendJson(exchange, 200, json.append("]}").toString());
    } catch (Exception e) {
      log("Fetch failed topic=" + topic + " partition=" + partition + " offset=" + offset + ": " + message(e));
      sendJson(exchange, 400, "{\"ok\":false,\"error\":" + quote(message(e)) + "}");
    }
  }

  /**
   * 返回**某个 broker 自己认为**的集群元数据（而不是 ZooKeeper 里的元数据）。
   *
   * <p>排查"元数据看起来正常但请求失败"这类问题时非常有用：
   * 可以对比 ZooKeeper 的分配与 broker 内存中的 leader / followers 是否一致。
   */
  private void brokerMetadata(HttpExchange exchange) throws IOException {
    if (!requireGet(exchange)) return;
    Map<String, String> query = query(exchange.getRequestURI());
    int port = intValue(query, "port", DEFAULT_BASE_PORT);
    try {
      SimpleKafkaClient client = new SimpleKafkaClient("localhost", port);
      client.initialize();
      StringBuilder json = new StringBuilder("{\"ok\":true,\"port\":").append(port).append(",\"brokers\":[");
      boolean first = true;
      for (Map.Entry<Integer, com.simplekafka.broker.BrokerInfo> entry : client.getBrokers().entrySet()) {
        if (!first) json.append(',');
        first = false;
        json.append("{\"id\":").append(entry.getKey())
            .append(",\"address\":").append(quote(entry.getValue().getHost() + ":" + entry.getValue().getPort()))
            .append('}');
      }
      json.append("],\"topics\":[");
      first = true;
      for (Map.Entry<String, SimpleKafkaClient.TopicMetadata> entry : client.getTopicMetadata().entrySet()) {
        if (!first) json.append(',');
        first = false;
        json.append("{\"name\":").append(quote(entry.getKey())).append(",\"partitions\":[");
        boolean firstPartition = true;
        for (SimpleKafkaClient.PartitionInfo partition : entry.getValue().getPartitions()) {
          if (!firstPartition) json.append(',');
          firstPartition = false;
          json.append("{\"id\":").append(partition.getId())
              .append(",\"leader\":").append(partition.getLeader())
              .append(",\"followers\":\"").append(partition.getFollowers()).append("\"}");
        }
        json.append("]}");
      }
      json.append("]}");
      sendJson(exchange, 200, json.toString());
    } catch (Exception e) {
      sendJson(exchange, 400, "{\"ok\":false,\"error\":" + quote(message(e)) + "}");
    }
  }

  private void createTopic(HttpExchange exchange) throws IOException {
    if (!requirePost(exchange)) return;
    Map<String, String> body = body(exchange);
    try {
      int bootstrapPort = controllerPort(intValue(body, "zkPort", this.zkPort),
          intValue(body, "port", DEFAULT_BASE_PORT));
      SimpleKafkaClient client = new SimpleKafkaClient("localhost", bootstrapPort);
      int partitions = intValue(body, "partitions", 3);
      short replicationFactor = (short) intValue(body, "replicationFactor", 2);
      boolean created = client.createTopic(body.get("topic"), partitions, replicationFactor);
      if (!created) {
        throw new IOException("create topic failed (分区数或副本数超出当前 broker 数量?)");
      }
      log("Created topic=" + body.get("topic") + " partitions=" + partitions + " rf=" + replicationFactor);
      sendJson(exchange, 200, "{\"ok\":true}");
    } catch (Exception e) {
      sendJson(exchange, 400, "{\"ok\":false,\"error\":" + quote(message(e)) + "}");
    }
  }

  /** 从 ZooKeeper 元数据中定位 controller 并返回其端口；controller 失效时清除陈旧节点等待重新选举。 */
  private int controllerPort(int port, int fallback) throws IOException {
    ZookeeperClient client = new ZookeeperClient("localhost", port);
    try {
      client.connect();
      for (int attempt = 0; attempt < 30; attempt++) {
        if (client.exists("/controller")) {
          int controllerId = Integer.parseInt(client.getData("/controller").trim());
          String brokerPath = "/brokers/" + controllerId;
          if (client.exists(brokerPath)) {
            String[] parts = client.getData(brokerPath).split(":", 2);
            if (parts.length == 2) return Integer.parseInt(parts[1]);
          } else {
            client.deleteNode("/controller");
          }
        }
        Thread.sleep(100L);
      }
      throw new IOException("controller election did not complete");
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("interrupted while locating controller", e);
    } catch (Exception e) {
      throw new IOException("failed to locate controller", e);
    } finally {
      try { client.close(); } catch (Exception ignored) { }
    }
  }

  // ============================== 进程管理 ==============================

  private void startZooKeeper(int port) {
    ManagedProcess existing = processes.get("zookeeper");
    if (existing != null && existing.isAlive()) return;
    processes.remove("zookeeper", existing);
    if (!portAvailable(port)) {
      log("zookeeper already running on port " + port + "; reusing it");
      return;
    }
    try {
      Path config = Files.createTempFile("simple-kafka-zk-", ".cfg");
      Files.writeString(config, "tickTime=2000\ndataDir=/tmp/simple-kafka-zk-dashboard\n"
          + "clientPort=" + port + "\nmaxClientCnxns=60\nadmin.enableServer=false\n");
      String cp = System.getProperty("java.class.path");
      // ZooKeeper 服务端默认按 INFO/DEBUG 输出大量日志，这里降到 WARN，避免刷满事件面板
      ManagedProcess process = new ManagedProcess("zookeeper",
          List.of("org.apache.zookeeper.server.quorum.QuorumPeerMain", config.toString()), cp, -1, -1, port,
          List.of("-Dzookeeper.root.logger=WARN,CONSOLE"));
      processes.put("zookeeper", process);
      process.start();    } catch (IOException e) { log("ZooKeeper start failed: " + message(e)); }
  }

  private void startBroker(int id, int port, int zkPort) {
    String name = "broker-" + id;
    ManagedProcess existing = processes.get(name);
    if (existing != null && existing.isAlive()) return;
    processes.remove(name, existing);
    if (!portAvailable(port)) {
      log(name + " already running on port " + port + "; reusing it");
      return;
    }
    String cp = System.getProperty("java.class.path");
    ManagedProcess process = new ManagedProcess(name,
        List.of("com.simplekafka.broker.SimpleKafkaBroker", String.valueOf(id), "localhost",
            String.valueOf(port), String.valueOf(zkPort)), cp, id, port, zkPort);
    processes.put(name, process);
    process.start();
  }

  private boolean portAvailable(int port) {
    try (java.net.ServerSocket socket = new java.net.ServerSocket()) {
      socket.setReuseAddress(true);
      socket.bind(new InetSocketAddress("localhost", port));
      return true;
    } catch (IOException e) {
      return false;
    }
  }

  /** 等待某个端口开始接受连接；用于 ZooKeeper 冷启动后再批量拉起 broker，避免注册竞争失败。 */
  private void waitForPort(int port, long timeoutMs) {
    long deadline = System.currentTimeMillis() + timeoutMs;
    while (System.currentTimeMillis() < deadline) {
      if (!portAvailable(port)) return;
      try {
        Thread.sleep(200L);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
    }
  }

  // ============================== ZooKeeper 中心操控接口 ==============================

  private void zkTree(HttpExchange exchange) throws IOException {
    if (!requireGet(exchange)) return;
    Map<String, String> query = query(exchange.getRequestURI());
    String root = query.getOrDefault("path", "/");
    ZookeeperClient client = new ZookeeperClient("localhost", intValue(query, "zkPort", this.zkPort));
    try {
      client.connect();
      StringBuilder json = new StringBuilder();
      appendTreeNode(client, root, json, 0);
      sendJson(exchange, 200, json.toString());
    } catch (Exception e) {
      sendJson(exchange, 400, "{\"ok\":false,\"error\":" + quote(message(e)) + "}");
    } finally {
      try { client.close(); } catch (Exception ignored) { }
    }
  }

  private void appendTreeNode(ZookeeperClient client, String path, StringBuilder json, int depth)
      throws KeeperException, InterruptedException {
    json.append("{\"path\":").append(quote(path));
    if (client.exists(path)) {
      json.append(",\"data\":").append(quote(client.getData(path)));
    }
    List<String> children = depth >= 8 ? Collections.emptyList() : client.getChildren(path);
    json.append(",\"children\":[");
    for (int i = 0; i < children.size(); i++) {
      if (i > 0) json.append(',');
      String child = "/".equals(path) ? "/" + children.get(i) : path + "/" + children.get(i);
      appendTreeNode(client, child, json, depth + 1);
    }
    json.append("]}");
  }

  private void zkRead(HttpExchange exchange) throws IOException {
    if (!requireGet(exchange)) return;
    Map<String, String> query = query(exchange.getRequestURI());
    String path = query.getOrDefault("path", "/");
    ZookeeperClient client = new ZookeeperClient("localhost", intValue(query, "zkPort", this.zkPort));
    try {
      client.connect();
      StringBuilder json = new StringBuilder("{\"ok\":true,\"path\":").append(quote(path));
      if (client.exists(path)) {
        json.append(",\"data\":").append(quote(client.getData(path)));
      }
      json.append(",\"children\":[");
      List<String> children = client.getChildren(path);
      for (int i = 0; i < children.size(); i++) {
        if (i > 0) json.append(',');
        json.append(quote(children.get(i)));
      }
      json.append("]}");
      sendJson(exchange, 200, json.toString());
    } catch (Exception e) {
      sendJson(exchange, 400, "{\"ok\":false,\"error\":" + quote(message(e)) + "}");
    } finally {
      try { client.close(); } catch (Exception ignored) { }
    }
  }

  private void zkCreate(HttpExchange exchange) throws IOException {
    if (!requirePost(exchange)) return;
    Map<String, String> body = body(exchange);
    String path = body.get("path");
    String data = body.getOrDefault("data", "");
    boolean ephemeral = "true".equals(body.get("ephemeral"));
    ZookeeperClient client = new ZookeeperClient("localhost", intValue(body, "zkPort", this.zkPort));
    if (path == null || path.isEmpty() || path.charAt(0) != '/') {
      sendJson(exchange, 400, "{\"ok\":false,\"error\":\"path must be absolute\"}");
      return;
    }
    try {
      client.connect();
      client.createPath(parentOf(path));
      if (client.exists(path)) {
        sendJson(exchange, 400, "{\"ok\":false,\"error\":\"node already exists\"}");
      } else {
        if (ephemeral) client.createEphemeralNode(path, data);
        else client.createPersistentNode(path, data);
        log("ZK created node " + path);
        sendJson(exchange, 200, "{\"ok\":true}");
      }
    } catch (Exception e) {
      sendJson(exchange, 400, "{\"ok\":false,\"error\":" + quote(message(e)) + "}");
    } finally {
      try { client.close(); } catch (Exception ignored) { }
    }
  }

  private static String parentOf(String path) {
    int index = path.lastIndexOf('/');
    return index <= 0 ? "/" : path.substring(0, index);
  }

  private void zkSet(HttpExchange exchange) throws IOException {
    if (!requirePost(exchange)) return;
    Map<String, String> body = body(exchange);
    String path = body.get("path");
    String data = body.getOrDefault("data", "");
    ZookeeperClient client = new ZookeeperClient("localhost", intValue(body, "zkPort", this.zkPort));
    try {
      client.connect();
      if (!client.exists(path)) {
        sendJson(exchange, 400, "{\"ok\":false,\"error\":\"node does not exist\"}");
      } else {
        client.setData(path, data);
        log("ZK updated node " + path);
        sendJson(exchange, 200, "{\"ok\":true}");
      }
    } catch (Exception e) {
      sendJson(exchange, 400, "{\"ok\":false,\"error\":" + quote(message(e)) + "}");
    } finally {
      try { client.close(); } catch (Exception ignored) { }
    }
  }

  private void zkDelete(HttpExchange exchange) throws IOException {
    if (!requirePost(exchange)) return;
    Map<String, String> body = body(exchange);
    String path = body.get("path");
    boolean recursive = "true".equals(body.get("recursive"));
    ZookeeperClient client = new ZookeeperClient("localhost", intValue(body, "zkPort", this.zkPort));
    try {
      client.connect();
      if ("/".equals(path)) {
        sendJson(exchange, 400, "{\"ok\":false,\"error\":\"cannot delete root\"}");
      } else {
        if (recursive) deleteRecursively(client, path);
        else client.deleteNode(path);
        log("ZK deleted node " + path + (recursive ? " (recursive)" : ""));
        sendJson(exchange, 200, "{\"ok\":true}");
      }
    } catch (Exception e) {
      sendJson(exchange, 400, "{\"ok\":false,\"error\":" + quote(message(e)) + "}");
    } finally {
      try { client.close(); } catch (Exception ignored) { }
    }
  }

  private static void deleteRecursively(ZookeeperClient client, String path)
      throws KeeperException, InterruptedException {
    if (!client.exists(path)) return;
    for (String child : client.getChildren(path)) {
      deleteRecursively(client, "/".equals(path) ? "/" + child : path + "/" + child);
    }
    client.deleteNode(path);
  }

  // ============================== 基础设施 ==============================

  private void log(String message) {
    String entry = "{\"id\":" + eventSequence.incrementAndGet() + ",\"time\":"
        + quote(java.time.OffsetDateTime.now().toString()) + ",\"message\":" + quote(message) + "}";
    eventLog.add(entry);
    if (eventLog.size() > 500) eventLog.remove(0);
  }

  private static Map<String, String> body(HttpExchange exchange) throws IOException {
    String raw = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
    Map<String, String> result = new HashMap<>();
    for (String item : raw.split("&")) {
      String[] pair = item.split("=", 2);
      if (pair.length == 2) result.put(pair[0], java.net.URLDecoder.decode(pair[1], StandardCharsets.UTF_8));
    }
    return result;
  }

  private static Map<String, String> query(URI uri) {
    try { return parseQuery(uri.getRawQuery()); } catch (Exception e) { return Collections.emptyMap(); }
  }

  private static Map<String, String> parseQuery(String raw) {
    Map<String, String> result = new HashMap<>();
    if (raw == null) return result;
    for (String item : raw.split("&")) {
      String[] pair = item.split("=", 2);
      if (pair.length == 2) result.put(pair[0], java.net.URLDecoder.decode(pair[1], StandardCharsets.UTF_8));
    }
    return result;
  }

  private static int intValue(Map<String, String> map, String key, int fallback) {
    try { return Integer.parseInt(map.getOrDefault(key, String.valueOf(fallback)).trim()); } catch (Exception e) { return fallback; }
  }

  private static long longValue(Map<String, String> map, String key, long fallback) {
    try { return Long.parseLong(map.getOrDefault(key, String.valueOf(fallback)).trim()); } catch (Exception e) { return fallback; }
  }

  private static String message(Throwable e) { return e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage(); }

  /** 完整转义 JSON 字符串，包含所有控制字符，避免前端解析失败。 */
  private static String quote(String value) {
    if (value == null) return "null";
    StringBuilder json = new StringBuilder(value.length() + 16).append('"');
    for (int i = 0; i < value.length(); i++) {
      char c = value.charAt(i);
      switch (c) {
        case '"': json.append("\\\""); break;
        case '\\': json.append("\\\\"); break;
        case '\n': json.append("\\n"); break;
        case '\r': json.append("\\r"); break;
        case '\t': json.append("\\t"); break;
        default:
          if (c < 0x20) {
            json.append(String.format("\\u%04x", (int) c));
          } else {
            json.append(c);
          }
      }
    }
    return json.append('"').toString();
  }
  private static void sendJson(HttpExchange exchange, int status, String json) throws IOException { send(exchange, status, "application/json; charset=utf-8", json); }
  private static void send(HttpExchange exchange, int status, String contentType, String content) throws IOException {
    byte[] bytes = content.getBytes(StandardCharsets.UTF_8);
    exchange.getResponseHeaders().set("Content-Type", contentType);
    exchange.getResponseHeaders().set("Cache-Control", "no-store");
    exchange.sendResponseHeaders(status, bytes.length);
    try (OutputStream output = exchange.getResponseBody()) { output.write(bytes); }
  }
  private static boolean requireGet(HttpExchange exchange) throws IOException { if (!"GET".equals(exchange.getRequestMethod())) { send(exchange, 405, "text/plain", "Method Not Allowed"); return false; } return true; }
  private static boolean requirePost(HttpExchange exchange) throws IOException { if (!"POST".equals(exchange.getRequestMethod())) { send(exchange, 405, "text/plain", "Method Not Allowed"); return false; } return true; }

  /** 控制台托管的进程：可能是自己启动的，也可能是接管的外部进程。 */
  private final class ManagedProcess {
    private final String name;
    private final List<String> command;
    private final String classpath;
    private final int brokerId;
    private final int brokerPort;
    private final int zkPort;
    private final List<String> jvmArgs;
    private volatile Process process;
    private volatile ProcessHandle handle;
    private volatile String lastOutput = "";

    private ManagedProcess(String name, List<String> command, String classpath,
        int brokerId, int brokerPort, int zkPort) {
      this(name, command, classpath, brokerId, brokerPort, zkPort, List.of());
    }

    private ManagedProcess(String name, List<String> command, String classpath,
        int brokerId, int brokerPort, int zkPort, List<String> jvmArgs) {
      this.name = name;
      this.command = command;
      this.classpath = classpath;
      this.brokerId = brokerId;
      this.brokerPort = brokerPort;
      this.zkPort = zkPort;
      this.jvmArgs = jvmArgs;
    }

    private String name() { return name; }
    private boolean isBroker() { return brokerId > 0; }

    private boolean isAlive() {
      Process child = process;
      if (child != null && child.isAlive()) return true;
      ProcessHandle external = handle;
      return external != null && external.isAlive();
    }

    private long pid() {
      Process child = process;
      if (child != null) return child.pid();
      ProcessHandle external = handle;
      return external == null ? -1L : external.pid();
    }

    private synchronized void start() {
      if (isAlive()) return;
      try {
        List<String> args = new ArrayList<>();
        args.add(System.getProperty("java.home") + "/bin/java");
        args.addAll(jvmArgs);
        args.add("-cp"); args.add(classpath); args.addAll(command);
        process = new ProcessBuilder(args).redirectErrorStream(true).start();
        handle = process.toHandle();
        Thread output = new Thread(() -> readOutput(process), name + "-output");
        output.setDaemon(true); output.start();
        log(name + " started");
      } catch (IOException e) { log(name + " failed to start: " + message(e)); }
    }

    private void readOutput(Process child) {
      // 按“完整行”缓冲：子进程输出是按 4KB 块读取的，一条长日志可能跨越两个块，
      // 若直接对每块 split 换行，就会产生 `]` 这种尾部碎片并刷满日志面板。
      StringBuilder pending = new StringBuilder();
      try (InputStream input = child.getInputStream()) {
        byte[] buffer = new byte[4096];
        int length;
        while ((length = input.read(buffer)) >= 0) {
          if (length <= 0) continue;
          pending.append(new String(buffer, 0, length, StandardCharsets.UTF_8));
          int lineStart = 0;
          int newline;
          while ((newline = pending.indexOf("\n", lineStart)) >= 0) {
            emitOutputLine(pending.substring(lineStart, newline));
            lineStart = newline + 1;
          }
          pending.delete(0, lineStart);
        }
        if (pending.length() > 0) {
          emitOutputLine(pending.toString());
        }
      } catch (IOException e) { log(name + " output closed"); }
    }

    /** 记录一行子进程输出：过滤噪声日志，其余写入事件流。 */
    private void emitOutputLine(String rawLine) {
      String line = rawLine.stripTrailing();
      if (line.isBlank()) return;
      lastOutput = (lastOutput + line + "\n");
      if (lastOutput.length() > 4000) {
        lastOutput = lastOutput.substring(lastOutput.length() - 4000);
      }
      if (isNoisyLogLine(line)) return;
      log(name + ": " + line);
    }

    /**
     * 停止进程。
     *
     * <p>优雅停止（SIGTERM）后等待 4 秒；若进程仍未退出（例如旧版本 broker 卡在关闭 ZooKeeper 的
     * 流程里），自动升级为强杀，保证「停止全部」「移除」一定生效。
     *
     * @param force true 表示直接强杀，不做优雅等待
     * @return true 表示最终是通过强杀才停下的
     */
    private synchronized boolean stop(boolean force) {
      List<ProcessHandle> targets = aliveTargets();
      if (targets.isEmpty()) return false;

      if (force) {
        for (ProcessHandle target : targets) target.destroyForcibly();
        waitGone(targets, 3000L);
        log(name + " killed");
        return true;
      }

      for (ProcessHandle target : targets) target.destroy();
      if (waitGone(targets, 4000L)) {
        log(name + " stopped");
        return false;
      }
      for (ProcessHandle target : targets) target.destroyForcibly();
      waitGone(targets, 3000L);
      log(name + " force stopped (graceful shutdown timed out)");
      return true;
    }

    /** 收集当前仍然存活的句柄（子进程句柄与接管句柄可能是同一个）。 */
    private List<ProcessHandle> aliveTargets() {
      List<ProcessHandle> targets = new ArrayList<>();
      Process child = process;
      if (child != null && child.isAlive()) targets.add(child.toHandle());
      ProcessHandle external = handle;
      if (external != null && external.isAlive()
          && (child == null || external.pid() != child.pid())) {
        targets.add(external);
      }
      return targets;
    }

    private boolean waitGone(List<ProcessHandle> targets, long timeoutMs) {
      long deadline = System.currentTimeMillis() + timeoutMs;
      while (System.currentTimeMillis() < deadline) {
        boolean anyAlive = false;
        for (ProcessHandle target : targets) {
          if (target.isAlive()) {
            anyAlive = true;
            break;
          }
        }
        if (!anyAlive) return true;
        try {
          Thread.sleep(150L);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          return false;
        }
      }
      return false;
    }

    private void restart() { start(); }

    private String json() {
      boolean alive = isAlive();
      long pid = pid();
      return "{\"name\":" + quote(name) + ",\"alive\":" + alive
          + ",\"pid\":" + (pid > 0 ? pid : "null")
          + ",\"brokerId\":" + (brokerId > 0 ? String.valueOf(brokerId) : "null")
          + ",\"port\":" + (brokerPort > 0 ? String.valueOf(brokerPort) : "null")
          + ",\"controllable\":" + (process != null || handle != null)
          + ",\"output\":" + quote(lastOutput) + "}";
    }
  }
}
