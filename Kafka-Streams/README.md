# Kafka-Streams

Kafka Streams 流处理学习模块：智慧家庭 IoT 场景下，演示 **窗口聚合（Tumbling Window + aggregate）** 与 **Stream-Stream Join**。

本模块**不依赖 PostgreSQL**，通过 Simulator / HTTP 注入输入事件，由 Kafka Streams 实时计算，结果写入输出 Topic，并由日志 Consumer 打印，便于本地验收。

---

## 设计思想

两条流水线注册在同一 `application-id`（`kafka-streams-learn`）下：

```
SmartHomeSimulator / HTTP 注入
    │
    ├─ 流水线 1（RoomTemperatureTopology）
    │   device.telemetry.temperature
    │       → 过滤合法温度
    │       → groupBy(roomId) + 1 分钟 Tumbling Window
    │       → aggregate(RoomTempAccumulator)
    │       ├─→ streams.room-temp-stats      （窗口统计）
    │       └─→ streams.temp-threshold-alert （均温 > 30°C）
    │
    └─ 流水线 2（IntrusionDetectionTopology）
        device.event.door  ×  device.event.motion
            → 过滤 OPEN / detected=true
            → 30 秒 Stream-Stream Join（homeId）
            └─→ streams.intrusion-alert

StreamsOutputLogConsumer 订阅三个输出 Topic → 控制台 [STATS] / [TEMP-ALERT] / [INTRUSION]
```

与 FanOut 的边界：**FanOut 消费已算好的告警并扇出**；**Streams 从原始遥测/门磁/人体事件流实时计算告警**。

---

## 为什么需要 Kafka Streams

| 需求 | 普通 Consumer 自实现 | Kafka Streams |
|------|---------------------|---------------|
| 每分钟房间均温 | 自维护样本列表 + 定时清理 | `TimeWindows` + `aggregate` |
| 门开 + 人体 30s 关联 | 双缓冲 + 乱序处理 | `stream-stream join` |
| 按 roomId 分组 | 手动保证 Key + repartition | `groupBy` 自动 repartition |
| 状态持久化 | 自建 Redis/DB | RocksDB State Store + changelog Topic |

---

## 用到的 Kafka 特性

| 特性 | 本模块用法 |
|------|------------|
| Kafka Streams DSL | `KStream` / `KTable` / `groupBy` / `windowedBy` / `join` |
| State Store | `room-temp-window-store`（窗口聚合中间状态） |
| Changelog / Repartition | Streams 自动创建 `kafka-streams-learn-*` 内部 Topic |
| 处理语义 | `processing.guarantee: at_least_once` |
| 多线程 | `num.stream.threads: 2` |
| 序列化 | 线上 JSON 字符串；Topology 内 `JsonSerde` + `EventJsonCodec` |
| 事件 Ingress | `KafkaTemplate`（Simulator / HTTP） |
| 结果 Egress | `.to(outputTopic)` + `@KafkaListener` 日志 Consumer |

> **说明**：Kafka Broker **不需要**额外开启 “Streams 模式”。标准 Kafka + 本模块 JVM 进程即可。

---

## Topic 一览

| Topic | 方向 | Record Key | 说明 |
|-------|------|------------|------|
| `device.telemetry.temperature` | 输入 | `deviceId` | 温湿度遥测 JSON |
| `device.event.door` | 输入 | `homeId` | 门磁 OPEN / CLOSED |
| `device.event.motion` | 输入 | `homeId` | 人体感应 detected |
| `streams.room-temp-stats` | 输出 | `roomId` | 分钟窗口统计 |
| `streams.temp-threshold-alert` | 输出 | `roomId` | 超温告警 |
| `streams.intrusion-alert` | 输出 | `homeId` | 入侵疑似告警 |

本地 Docker（`docker-compose.yml`）已开启 `KAFKA_AUTO_CREATE_TOPICS_ENABLE=true`，首次发送时会自动建 Topic。  
若连接远程 Kafka 且关闭了自动建 Topic，需手动创建上述 6 个 Topic。

---

## 项目结构

```
Kafka-Streams/src/main/java/com/kafkalearn/
├── KafkaStreamsApplication.java      # @EnableKafka + @EnableKafkaStreams
├── api/
│   └── EventInjectController.java    # HTTP 手动注入（L3–L6 补充）
├── config/
│   ├── StreamsTopologyConfig.java    # 注册两条 Topology
│   ├── KafkaAutoConfiguration.java   # KafkaTemplate / Listener 工厂
│   └── KafkaTopic.java               # Topic 名称常量
├── topology/
│   ├── RoomTemperatureTopology.java  # 流水线 1：窗口聚合
│   └── IntrusionDetectionTopology.java
├── accumulator/
│   └── RoomTempAccumulator.java      # 窗口内 count / sum / max
├── message/                          # 输入/输出 JSON 模型（record）
├── simulator/
│   └── SmartHomeSimulator.java       # L1–L6 + 异常样例 + 周期遥测
├── producer/
│   └── InputEventPublisher.java      # 异步写入输入 Topic
├── consumer/
│   └── StreamsOutputLogConsumer.java # 订阅输出 Topic 打日志
└── messaging/
    └── EventJsonCodec.java           # JSON ↔ 对象
```

---

## 运行

### 1. 启动 Kafka

```bash
# 项目根目录
docker compose up -d
```

### 2. 指定 Kafka 地址（推荐）

`application.yml` 默认 `bootstrap-servers` 为 `192.168.19.64:9092`。  
使用本机 Docker 时请覆盖：

```bash
# Linux / macOS
export APP_KAFKA_BOOTSTRAP_SERVERS=localhost:9092

# Windows PowerShell
$env:APP_KAFKA_BOOTSTRAP_SERVERS="localhost:9092"
```

### 3. 启动模块

```bash
mvn -pl Kafka-Streams spring-boot:run
```

或在模块目录：

```bash
cd Kafka-Streams
mvn spring-boot:run
```

| 项 | 值 |
|----|-----|
| 主类 | `com.kafkalearn.KafkaStreamsApplication` |
| 默认 HTTP 端口 | `8082` |
| 日志文件 | `./Kafka-Streams/log/` |

启动后约 **12 秒**，`SmartHomeSimulator` 会自动跑一遍 L1–L6 与异常样例；之后每 **5 秒** 持续发送正常遥测。

---

## 关键配置

### 环境变量

| 环境变量 | 默认值 | 说明 |
|----------|--------|------|
| `APP_SERVER_PORT` | `8082` | HTTP 端口 |
| `APP_KAFKA_BOOTSTRAP_SERVERS` | `192.168.19.64:9092` | Kafka 地址 |
| `SPRING_KAFKA_STREAMS_APPLICATION_ID` | `kafka-streams-learn` | Streams 应用 ID（即 Consumer Group） |
| `LOGGIN_FILE_PATH` | `./Kafka-Streams/log` | 日志目录 |

### application.yml 业务参数

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `app.streams.temp-threshold-celsius` | `30.0` | 超温阈值（°C） |
| `app.streams.temp-window-minutes` | `1` | 温度聚合 Tumbling 窗口（分钟） |
| `app.streams.intrusion-join-seconds` | `30` | 门磁 × 人体 Join 时间差窗口（秒） |
| `app.simulator.enabled` | `true` | 是否启用 Simulator |
| `app.simulator.telemetry-interval-ms` | `5000` | 周期遥测间隔 |
| `app.simulator.kitchen-overheat-enabled` | `true` | 是否持续发厨房超温（L2） |
| `app.simulator.kitchen-overheat-temperature` | `32.0` | 厨房超温样例温度 |
| `app.simulator.scenario-demo-enabled` | `true` | 启动后是否跑一遍 L1–L6 |
| `app.simulator.scenario-startup-delay-ms` | `12000` | 场景演示延迟（等 Streams RUNNING） |
| `app.simulator.join-timeout-demo-gap-ms` | `35000` | L4 Join 超时演示间隔 |

关闭一次性场景演示（仅保留周期遥测）：

```yaml
app:
  simulator:
    scenario-demo-enabled: false
```

重置 Streams 状态（开发环境消费组已提交 offset、想从头重跑）：

```bash
# 改 application-id 即可视为全新应用
export SPRING_KAFKA_STREAMS_APPLICATION_ID=kafka-streams-learn-v2
```

---

## 学习场景（L1–L6）

`SmartHomeSimulator` 启动约 12s 后自动执行下表；日志前缀 `[SIM-*]` 标识当前场景。

| 编号 | 场景 | Simulator 行为 | 期望日志 |
|------|------|----------------|----------|
| L1 | 正常聚合 | 客厅 / 卧室正常温度 | `[STATS-TOPO]` / `[STATS] room-living`、`room-bedroom` |
| L2 | 超温告警 | 厨房 32°C | `[TEMP-ALERT-TOPO]` / `[TEMP-ALERT] room-kitchen` |
| L3 | Join 成功 | 门开 + 1s 内人体 | `[INTRUSION-TOPO]` / `[INTRUSION] home=home-001` |
| L4 | Join 超时 | 门开，35s 后人体 | **无** `[INTRUSION]` |
| L5 | 仅门开 | 只发 door OPEN | **无** `[INTRUSION]` |
| L6 | 窗口边界 | 同房间连发 3 条温度 | `[STATS] ... sampleCount=3` |
| 异常 | 非法数据 | 见下表 | **无** 对应 streams 输出；可能出现反序列化 WARN |

### 异常样例（故意触发，WARN 属预期）

| 样例 | 行为 | Topology 处理 |
|------|------|---------------|
| 温度 -50°C / 85°C | 超出 [-40, 80] | `filter` 丢弃 |
| `temperatureCelsius: null` | 非法数值 | `filter` 丢弃 |
| 残缺 JSON `{deviceId:"broken"...` | 反序列化失败 | 返回 null → `filter` 丢弃 |
| 缺 `roomId` 的 JSON | 反序列化成功但 roomId 为空 | groupBy 行为异常，不应产生有效 stats |
| 门磁 `CLOSED` | 非 OPEN | Join 流水线 `filter` 丢弃 |
| `detected: false` | 无人体 | Join 流水线 `filter` 丢弃 |
| 残缺门磁 JSON | 反序列化失败 | 丢弃 |

---

## HTTP 手动注入

除 Simulator 外，可通过 HTTP 补充触发（返回 `202 Accepted`，异步发送）：

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/api/inject/temperature` | 温湿度遥测 |
| POST | `/api/inject/door` | 门磁事件 |
| POST | `/api/inject/motion` | 人体感应 |

### L3 示例（curl）

```bash
curl -X POST http://localhost:8082/api/inject/door \
  -H "Content-Type: application/json" \
  -d '{"deviceId":"door-entry","homeId":"home-001","state":"OPEN","occurredAt":1710000000000}'

curl -X POST http://localhost:8082/api/inject/motion \
  -H "Content-Type: application/json" \
  -d '{"deviceId":"motion-living","homeId":"home-001","roomId":"room-living","detected":true,"occurredAt":1710000001000}'
```

### 温度示例

```bash
curl -X POST http://localhost:8082/api/inject/temperature \
  -H "Content-Type: application/json" \
  -d '{"deviceId":"sensor-kitchen-temp","roomId":"room-kitchen","homeId":"home-001","temperatureCelsius":80,"humidityPercent":100,"reportedAt":1710000000000}'
```

字段名须与 `message/*` record 一致（camelCase）。

---

## 如何验收

### 看控制台日志（推荐）

| 前缀 | 来源 | 含义 |
|------|------|------|
| `[SIM-*]` | SmartHomeSimulator | 样例发送进度 |
| `[STATS-TOPO]` | RoomTemperatureTopology | 窗口统计已写出 |
| `[TEMP-ALERT-TOPO]` | RoomTemperatureTopology | 超温告警已写出 |
| `[INTRUSION-TOPO]` | IntrusionDetectionTopology | 入侵告警已写出 |
| `[STATS]` / `[TEMP-ALERT]` / `[INTRUSION]` | StreamsOutputLogConsumer | 输出 Topic 已被消费 |

### 看 Kafka Topic

```bash
# 输入（应有消息）
docker exec kafka-learn /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic device.telemetry.temperature --from-beginning --max-messages 3

# 输出（Streams 跑通后应有消息）
docker exec kafka-learn /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic streams.room-temp-stats --from-beginning
```

### 看 Streams 消费进度

```bash
docker exec kafka-learn /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe --group kafka-streams-learn
```

---

## 常见问题

### 输入 Topic 有消息，输出 Topic 为空？

1. **确认 Kafka-Streams 应用在跑**，且 `bootstrap-servers` 与你看 Topic 的集群一致。  
2. **输入有消息 ≠ 输出有消息**：输出必须由 Streams Topology 计算写入。  
3. 检查消费组 lag：若 lag=0 且无新消息，旧数据可能已被消费但未产出（改 `application-id` 重跑）。  
4. 确认 `device.event.door`、`device.event.motion` 存在（入侵链路需要）。  
5. 看应用日志是否有 `State transition ... RUNNING` 和 `[STATS-TOPO]`。

### `UNKNOWN_TOPIC_OR_PARTITION`？

对应 Topic 在 Broker 上不存在。Docker 本地一般自动创建；远程 Kafka 需手动建 Topic 或开启 auto-create。

### `事件反序列化失败` WARN？

若为 `[SIM-ABNORMAL]` 故意发送的残缺 JSON，**属预期**，消息会被 Topology 丢弃，不会进入 `streams.*`。

### HTTP 返回 202 但没有输出？

202 只表示请求已接受；发送是异步的。若 Kafka 不可达，Producer 会失败且需看应用日志（`InputEventPublisher` 在 INFO 级别记录发送内容）。

---

## 与 FanOut / EventDriven 对比

| 模块 | 输入 | 核心能力 |
|------|------|----------|
| EventDriven | 设备上下线事件 | 单条事件更新状态 |
| FanOut | 已生成告警 | 一条告警多下游扇出 |
| **Kafka-Streams** | 原始遥测 / 门磁 / 人体 | 窗口统计 + 双流 Join |

---

## 测试

Topology 使用 `TopologyTestDriver` 做单元测试，不依赖真实 Kafka：

```bash
mvn -pl Kafka-Streams test
```

覆盖：`RoomTempAccumulatorTest`、`RoomTemperatureTopologyTest`、`IntrusionDetectionTopologyTest`。

---

## 延伸阅读（本模块未实现）

- Stream-Table Join（设备档案 KTable enrich 阈值）
- Join grace period 与乱序事件
- Session / Sliding Window
- `exactly_once_v2`
- `suppress(untilWindowCloses)`（每窗口只输出一条）
- 多实例 Streams rebalance

详细设计说明见：`docs/superpowers/specs/2026-06-14-kafka-streams-design.md`
