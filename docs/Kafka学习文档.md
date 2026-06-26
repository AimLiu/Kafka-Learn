# Kafka 学习文档

> 基于 [Kafka-Learn](https://github.com) 仓库整理的 Kafka 知识体系。  
> 技术栈：Java 21 · Spring Boot 3.3.5 · Spring Kafka · Kafka 3.9（Docker）· PostgreSQL  
> 文档版本与仓库模块同步，当前覆盖 **8 个已实现模块**。

---

## 目录

1. [Kafka 是什么](#1-kafka-是什么)
2. [核心概念速查](#2-核心概念速查)
3. [本仓库知识地图](#3-本仓库知识地图)
4. [按模块学习](#4-按模块学习)
5. [使用模式详解](#5-使用模式详解)
6. [可靠性、幂等与一致性](#6-可靠性幂等与一致性)
7. [Spring Kafka 实践要点](#7-spring-kafka-实践要点)
8. [Kafka Streams 专题](#8-kafka-streams-专题)
9. [物联网（IoT）场景](#9-物联网iot场景)
10. [拓展知识（待建模块）](#10-拓展知识待建模块)
11. [推荐学习路线](#11-推荐学习路线)
12. [配置与运维速查](#12-配置与运维速查)
13. [术语表](#13-术语表)

---

## 1. Kafka 是什么

Apache Kafka 是**分布式事件流平台**，核心能力：

| 能力 | 说明 |
|------|------|
| **发布 / 订阅** | 生产者写 Topic，消费者按组订阅 |
| **持久化** | 消息按配置保留在磁盘，可回溯 |
| **高吞吐** | 顺序写盘 + 分区并行 |
| **水平扩展** | 分区数决定并行度，Consumer Group 决定消费能力 |

### Kafka 不是什么

- **不是 RPC 框架** — Request-Reply 需应用层自己实现（见 `Kafka-RequestReply`）
- **不是数据库** — 事件溯源的真相源应在 DB（见 `Kafka-EventSourcing`）
- **不保证业务幂等** — at-least-once 下 Consumer 必须自己做去重

### 在本仓库中的定位

```
┌─────────────────────────────────────────┐
│  应用层（Spring Boot 各模块）              │
│  业务协议、状态机、幂等、补偿、投影         │
└─────────────────────────────────────────┘
                    ↕
┌─────────────────────────────────────────┐
│  Kafka（消息总线）                        │
│  Topic · Partition · Offset · Group    │
└─────────────────────────────────────────┘
                    ↕
┌─────────────────────────────────────────┐
│  PostgreSQL（部分模块）                  │
│  业务事实源、Outbox、Event Store、读模型   │
└─────────────────────────────────────────┘
```

---

## 2. 核心概念速查

### 2.1 Topic 与 Partition

- **Topic**：逻辑消息通道，如 `device-alert-topic`
- **Partition**：Topic 的物理分片，分区内**有序**，分区间**无序**
- **分区键（Key）**：相同 Key 进入同一分区 → 保证单实体消息有序（如 `deviceId`、`accountId`、`roomId`）

```
Topic: device-alert-topic (3 partitions)
  P0: [msg1, msg4, msg7]
  P1: [msg2, msg5]
  P2: [msg3, msg6]
```

### 2.2 Producer

- 向 Topic 发送 **Record**（Key + Value + Headers + Timestamp）
- 重要配置：`acks`、`retries`、`batch.size`、`linger.ms`、`compression.type`、`enable.idempotence`

### 2.3 Consumer 与 Consumer Group

- **Consumer Group**：组内消费者**竞争**分区，每条消息只被组内一个实例消费
- **不同 Group**：各自独立消费同一 Topic → **Fan-out / Pub-Sub**
- **Offset**：消费进度指针，提交方式决定 at-least-once / at-most-once

| 场景 | Group 策略 | 本仓库模块 |
|------|------------|------------|
| 点对点 / 任务分发 | 同 Group 多实例竞争 | `Kafka-PointToPoint` |
| 一事件多下游 | 不同 Group 各消费一次 | `Kafka-FanOut` |
| Request / Reply 分离 | 调用方与处理方不同 Group | `Kafka-RequestReply` |

### 2.4 投递语义

| 语义 | 含义 | 本仓库体现 |
|------|------|------------|
| **At-most-once** | 可能丢消息 | 一般不刻意使用 |
| **At-least-once** | 可能重复，不丢 | 大多数模块默认行为 |
| **Exactly-once** | 不丢不重（端到端难） | Streams 可配 `exactly_once_v2`（未在本仓库实现） |

### 2.5 Record Header

键值元数据，不进入 Value 体，适合放协议字段：

| Header | 用途 | 模块 |
|--------|------|------|
| `correlationId` | Request-Reply 配对 | `Kafka-RequestReply` |
| `replyTo` | 指定回复 Topic | `Kafka-RequestReply` |
| `sagaId` | Saga 追踪（拓展） | 待建 `Kafka-Saga` |

---

## 3. 本仓库知识地图

### 3.1 模块索引

| 模块 | 模式 | 端口 | 涉及 Kafka 特性 |
|------|------|------|-----------------|
| [Kafka-PointToPoint](../Kafka-PointToPoint/) | 点对点 / 竞争消费 | 8080 | Producer API、Consumer Group、JSON 序列化 |
| [Kafka-LogAggregation](../Kafka-LogAggregation/) | 日志聚合 | 8080 | 多源写入、Consumer 落库、主键幂等 |
| [Kafka-EventDriven](../Kafka-EventDriven/) | 事件驱动 | 8080 | 单向事件、acks=all、批处理压缩 |
| [Kafka-Outbox-Model](../Kafka-Outbox-Model/) | Transactional Outbox | 17070 | 可靠发消息、Relay 补偿、消费重试 |
| [Kafka-FanOut](../Kafka-FanOut/) | 扇出 + DLQ | 8081 | 多 Group、手动 ack、DLQ、Producer 幂等 |
| [Kafka-EventSourcing](../Kafka-EventSourcing/) | 事件溯源 + CQRS | 8080 | 写库后发事件、投影 Consumer、命令幂等 |
| [Kafka-RequestReply](../Kafka-RequestReply/) | 请求-响应 | 8081 | 双 Topic、Header、应用层 Future 配对 |
| [Kafka-Streams](../Kafka-Streams/) | 流处理 | 8082 | 窗口聚合、Stream Join、State Store |

### 3.2 Kafka 特性 × 模块对照矩阵

| Kafka 特性 | PointToPoint | LogAgg | EventDriven | Outbox | FanOut | EventSourcing | RequestReply | Streams |
|------------|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| Producer API / KafkaTemplate | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| @KafkaListener Consumer | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| Consumer Group 竞争消费 | ✓ | ✓ | ✓ | ✓ | — | ✓ | ✓ | — |
| 多 Consumer Group Fan-out | — | — | — | — | ✓ | — | — | — |
| 分区键策略 | — | — | — | message_key | — | — | accountId | deviceId/homeId/roomId |
| acks=all | — | — | ✓ | 可配 | ✓ | — | ✓ | — |
| enable.idempotence | — | — | — | — | ✓ | — | ✓ | — |
| 压缩 snappy/gzip | gzip | gzip | snappy | — | snappy | — | — | — |
| batch / linger | — | — | ✓ | — | ✓ | — | — | — |
| 手动 ack | — | — | — | — | ✓ | — | ✓ | — |
| DLQ | — | — | — | — | ✓ | — | — | — |
| Record Header | — | — | — | — | — | — | ✓ | — |
| 双 Topic Request-Reply | — | — | — | — | — | — | ✓ | — |
| Kafka Streams DSL | — | — | — | — | — | — | — | ✓ |
| 窗口聚合 | — | — | — | — | — | — | — | ✓ |
| Stream-Stream Join | — | — | — | — | — | — | — | ✓ |
| State Store | — | — | — | — | — | — | — | ✓ |
| PostgreSQL 协同 | — | ✓ | ✓ | ✓ | ✓ | ✓ | — | — |
| 消费幂等（业务键） | — | msg_id | eventId | task 状态 | event_id | commandId/event_id | correlationId | — |

### 3.3 模式覆盖状态

| # | 模式 | 状态 | 对应模块 |
|---|------|------|----------|
| 1 | 点对点 / 竞争消费 | **已有** | `Kafka-PointToPoint` |
| 2 | 发布/订阅 | 部分 | `Kafka-FanOut`、`Kafka-EventDriven` |
| 3 | 日志聚合 | **已有** | `Kafka-LogAggregation` |
| 4 | 事件驱动 | **已有** | `Kafka-EventDriven` |
| 5 | 事件溯源 | **已有** | `Kafka-EventSourcing` |
| 6 | CQRS | **已有** | 合并在 `Kafka-EventSourcing` |
| 7 | Fan-out | **已有** | `Kafka-FanOut` |
| 8 | Request-Reply | **已有** | `Kafka-RequestReply` |
| 9 | Kafka Streams | **已有** | `Kafka-Streams` |
| 10 | 幂等消费（专题） | 部分 | `Kafka-FanOut`、`Kafka-EventSourcing` |
| 11 | DLQ（专题） | 部分 | `Kafka-FanOut` |
| 12 | Outbox | **已有** | `Kafka-Outbox-Model` |
| 13 | Saga | 待建 | `Kafka-Saga` |
| 14 | CDC | 待建 | `Kafka-CDC` |

---

## 4. 按模块学习

### 4.1 Kafka-PointToPoint — 入门第一课

**学什么**：Producer → Topic → Consumer 最简链路；Consumer Group 决定消息分配。

```
定时调度 → KafkaTemplate 发送 → kafka-learn-producer
    → 同 Group 两个 @KafkaListener 竞争消费
```

**关键收获**：
- 同 Group = 每条消息只被一个实例处理（Queue-like）
- 不同 Group = 各消费一次（Pub-Sub）
- `CompletableFuture` 异步发送回调

**未涉及**：幂等、手动 ack、DLQ、数据库 — 刻意保持极简。

→ 详见 [Kafka-PointToPoint/README.md](../Kafka-PointToPoint/README.md)

---

### 4.2 Kafka-LogAggregation — 多源汇聚

**学什么**：多个生产者写入同一 Topic，统一 Consumer 落库。

**关键收获**：
- 日志聚合是 Pub/Sub 的特化（多写、单 Topic、统一消费）
- JSON 序列化在 Spring Kafka 中的 Producer/Consumer 配置
- `msg_id` 作为主键 → 消费幂等基础

→ 详见 [Kafka-LogAggregation/README.md](../Kafka-LogAggregation/README.md)

---

### 4.3 Kafka-EventDriven — 事件驱动解耦

**学什么**：设备上下线以事件发布，下游异步更新当前状态。

```
DeviceStatusSimulator → device-status-update-topic → 更新 device_status 表
```

**关键收获**：
- 「状态变更 = 事件」，发布方不关心下游数量
- Kafka 是传递通道，真相在消费侧**当前状态表**
- 与 EventSourcing 对比：无完整历史、不可重放

**Kafka 配置亮点**：`acks=all`、`retries=3`、`snappy` 压缩、`batch-size` + `linger-ms`

→ 详见 [Kafka-EventDriven/README.md](../Kafka-EventDriven/README.md)

---

### 4.4 Kafka-Outbox-Model — 可靠发消息

**学什么**：解决「写库成功、发 Kafka 失败」的不一致。

```
API → 同事务写 callback_task + callback_outbox
    → Outbox Relay 定时扫描 PENDING → 发 Kafka
    → Consumer 模拟第三方回调 → 回写状态 / 重试
```

**关键收获**：
- **DB 是事实源**，Kafka 是异步传输层
- Outbox 与业务表**同一事务**写入
- Relay 补偿应对 Kafka 短暂不可用
- 消费端结合任务状态机做幂等

**未涉及**：Kafka 事务 Producer、EOS、Schema Registry

→ 详见 [Kafka-Outbox-Model/README.md](../Kafka-Outbox-Model/README.md)

---

### 4.5 Kafka-FanOut — 扇出 + DLQ + 幂等

**学什么**：一条告警被三个 Consumer Group 并行处理；失败进 DLQ。

```
device-alert-topic
  ├─ alert-storage-group   → 存储
  ├─ alert-push-group      → 推送
  └─ alert-rule-group      → 规则引擎
  失败 → 各自 DLQ → 归档
```

**关键收获**：
- Fan-out 靠**不同 Consumer Group**，不是多 Topic
- `DefaultErrorHandler` + `DeadLetterPublishingRecoverer`
- 手动 ack：`enable-auto-commit: false` + `ack-mode: record`
- `event_id` 唯一索引 → at-least-once 下幂等
- Producer `enable-idempotence: true`

→ 详见 [Kafka-FanOut/README.md](../Kafka-FanOut/README.md)

---

### 4.6 Kafka-EventSourcing — 事件溯源 + CQRS

**学什么**：状态变更追加为不可变事件；Kafka 驱动读模型投影。

```
Command API → es_event（PG Event Store）
           → Kafka account-events
           → AccountEventProjector → 余额/流水读模型
```

**关键收获**：
- 写侧真相源是 **PostgreSQL `es_event`**，不是 Kafka
- Kafka 角色：事务提交后**分发事件、触发投影**
- 命令幂等：`es_command_dedup`（`commandId`）
- 投影幂等：按 `event_id` 去重
- 乐观锁：`current_version` 防并发冲突

→ 详见 [Kafka-EventSourcing/README.md](../Kafka-EventSourcing/README.md)

---

### 4.7 Kafka-RequestReply — 应用层 RPC

**学什么**：双 Topic + `correlationId` 在 Kafka 上模拟同步请求-响应。

```
HTTP → BalanceQueryClient（register Future + 发 request）
     → BalanceQueryRequestConsumer（处理 + 发 reply）
     → ReplyListener（complete Future）
```

**关键收获**：
- Request-Reply 是**应用层协议**，Kafka 不理解 request/response
- `ReplyListener` 属于**调用方**，把 Kafka 消息桥接为 `CompletableFuture`
- `PendingReplyRegistry.complete` 先 `remove` 再 `complete` → 重复 reply 丢弃
- 业务错误（`NOT_FOUND`）走 reply 消息；超时由 `future.get(timeout)` 触发

→ 详见 [Kafka-RequestReply/README.md](../Kafka-RequestReply/README.md)

---

### 4.8 Kafka-Streams — 流处理

**学什么**：窗口聚合 + Stream-Stream Join，从原始遥测实时计算告警。

```
device.telemetry.temperature → 1 分钟 Tumbling Window → 房间均温统计 / 超温告警
device.event.door × device.event.motion → 30s Join → 入侵告警
```

**关键收获**：
- `KStream` / `groupBy` / `windowedBy` / `aggregate`
- `stream-stream join` 替代手写双缓冲
- RocksDB State Store + changelog Topic（Streams 自动管理）
- `TopologyTestDriver` 单元测试不依赖真实 Kafka
- 与 FanOut 边界：Streams **计算**告警，FanOut **分发**告警

→ 详见 [Kafka-Streams/README.md](../Kafka-Streams/README.md)

---

## 5. 使用模式详解

### 5.1 点对点（Queue-like）

```
Producer → Topic → [Consumer A | Consumer B]  （同 Group，竞争）
```

- **场景**：任务队列、异步作业分发
- **模块**：`Kafka-PointToPoint`
- **扩展**：增加 Group 内实例数提高吞吐

### 5.2 发布/订阅（Pub/Sub）

```
Producer → Topic → Group-1（全量）
                 → Group-2（全量）
                 → Group-3（全量）
```

- **场景**：一事件通知多个独立系统
- **模块**：`Kafka-FanOut`、`Kafka-EventDriven`
- **关键**：Consumer Group 是 Pub/Sub 的实现机制

### 5.3 日志聚合

```
Service-A ─┐
Service-B ─┼→ log-topic → LogConsumer → 统一存储
Service-C ─┘
```

- **场景**：可观测性、ELK 管道前置
- **模块**：`Kafka-LogAggregation`

### 5.4 事件驱动

```
状态变更 → Event → 下游各自处理（无 reply）
```

- **场景**：微服务解耦、IoT 设备生命周期
- **模块**：`Kafka-EventDriven`
- **对比 RPC**：发完即走，不等待响应

### 5.5 Fan-out

```
一条告警 → 存储 + 推送 + 规则引擎（并行、互不影响）
```

- **场景**：订单触发多系统、告警多通道
- **模块**：`Kafka-FanOut`

### 5.6 Event Sourcing + CQRS

```
写：Command → Event Store（append-only）
读：Event → Projector → 读模型视图（异步）
```

- **场景**：审计、账户流水、需要重放的状态
- **模块**：`Kafka-EventSourcing`

### 5.7 Request-Reply

```
Caller → request-topic → Handler → reply-topic → Caller
         correlationId 贯穿全程
```

- **场景**：跨服务查询、需同步等待结果
- **模块**：`Kafka-RequestReply`
- **注意**：不适合替代所有 HTTP/gRPC；适合已有 Kafka 生态的异步 RPC

### 5.8 流处理（Kafka Streams）

```
原始事件流 → 窗口 / Join / 聚合 → 衍生事件流
```

- **场景**：实时统计、规则引擎、风控
- **模块**：`Kafka-Streams`

### 5.9 Transactional Outbox

```
业务写库 + Outbox 表（同事务）→ Relay → Kafka → Consumer
```

- **场景**：可靠通知第三方、开放平台回调
- **模块**：`Kafka-Outbox-Model`

---

## 6. 可靠性、幂等与一致性

### 6.1 三个层次

| 层次 | 机制 | 防什么 | 本仓库 |
|------|------|--------|--------|
| **传输层** | Producer `enable-idempotence` | 生产者重试导致 Broker 重复 record | FanOut、RequestReply |
| **投递层** | `acks=all`、手动 ack | 消息丢失 | FanOut、EventDriven、RequestReply |
| **业务层** | 幂等键 + DB 唯一约束 | Consumer 重复消费副作用 | FanOut、ES、LogAgg、RequestReply |

### 6.2 幂等常见做法

| 方式 | 说明 | 本仓库示例 |
|------|------|------------|
| 主键 / 唯一索引 | `INSERT` 冲突视为已处理 | `event_id`、`msg_id` |
| 幂等表 | `command_dedup` 记录处理状态 | `es_command_dedup` |
| 状态机 | 仅允许合法状态迁移 | `callback_task.status` |
| 内存去重 | `correlationId` → Future remove | `PendingReplyRegistry` |
| Redis NX | 高并发短时去重 | 未实现 |

### 6.3 DLQ（死信队列）

```
消费失败 → 重试 N 次 → DeadLetterPublishingRecoverer → DLQ Topic → 归档 / 人工处理
```

- **模块**：`Kafka-FanOut`
- **配置**：`DefaultErrorHandler` + `FixedBackOff`
- **不可重试异常**：`JsonProcessingException` 直接进 DLQ

### 6.4 Outbox 与 Kafka 的一致性

| 方案 | 说明 |
|------|------|
| **Transactional Outbox** | 本仓库 `Kafka-Outbox-Model` |
| **CDC（Debezium）** | 监听 Outbox 表变更发 Kafka（拓展） |
| **Kafka 事务 Producer** | 跨 Topic 原子写，复杂度高（本仓库未用） |

### 6.5 Request-Reply 中的重复

| 重复来源 | 应对位置 |
|----------|----------|
| Producer send 重试 | `enable-idempotence` |
| Request 重复消费 | server 端 `correlationId` 去重（建议补） |
| Reply 重复投递 | `PendingReplyRegistry.complete` 先 remove |
| HTTP 用户重试 | 客户端 `Idempotency-Key`（未实现） |

---

## 7. Spring Kafka 实践要点

### 7.1 常用组件

| 组件 | 用途 |
|------|------|
| `KafkaTemplate` | 发送消息 |
| `@KafkaListener` | 声明式消费 |
| `ProducerFactory` / `ConsumerFactory` | 底层客户端配置 |
| `ConcurrentKafkaListenerContainerFactory` | 监听容器、错误处理、ack 模式 |
| `DefaultErrorHandler` | 消费失败重试 + DLQ |
| `DeadLetterPublishingRecoverer` | 死信发布 |
| `@EnableKafkaStreams` | 启用 Streams 拓扑 |

### 7.2 序列化策略（本仓库）

| 方式 | 模块 | 说明 |
|------|------|------|
| `JsonSerializer` / `JsonDeserializer` | PointToPoint、LogAgg | Spring 内置 JSON |
| `StringSerializer` + 自定义 Codec | EventDriven、FanOut、ES、RequestReply | JSON 字符串 + 手写编解码 |
| `JsonSerde` | Kafka-Streams | Topology 内序列化 |

### 7.3 Consumer 配置模式

```yaml
# 自动提交（简单场景）
enable-auto-commit: true

# 手动 ack（可靠性要求高）
enable-auto-commit: false
listener:
  ack-mode: record    # 每条处理完再 ack
```

### 7.4 Producer 可靠性配置模板

```yaml
producer:
  acks: all
  retries: 3
  enable-idempotence: true
  compression-type: snappy
  batch-size: 16384
  linger-ms: 10
```

### 7.5 错误处理模板（FanOut 模式）

```java
// DefaultErrorHandler + FixedBackOff + DeadLetterPublishingRecoverer
// 见 Kafka-FanOut/config/KafkaAutoConfiguration.java
```

---

## 8. Kafka Streams 专题

### 8.1 核心概念

| 概念 | 说明 |
|------|------|
| **KStream** | 不可变事件流 |
| **KTable** | 可变 changelog 流（最新状态） |
| **State Store** | 本地 RocksDB，容错靠 changelog Topic |
| **Repartition** | `groupBy` 改变 Key 时自动创建 |
| **application.id** | 即 Consumer Group，改 ID = 全新应用 |

### 8.2 本模块两条流水线

**流水线 1：房间温度窗口聚合**

```
temperature → filter → groupBy(roomId) → 1min Tumbling Window
    → aggregate → room-temp-stats + temp-threshold-alert
```

**流水线 2：入侵检测 Join**

```
door(OPEN) × motion(detected=true) → 30s Stream-Stream Join(homeId) → intrusion-alert
```

### 8.3 与普通 Consumer 对比

| 需求 | 手写 Consumer | Kafka Streams |
|------|---------------|---------------|
| 分钟级聚合 | 自维护窗口 + 定时清理 | `TimeWindows` + `aggregate` |
| 双流关联 | 双缓冲 + 乱序处理 | `join` |
| 状态持久化 | 自建 Redis/DB | RocksDB + changelog |

### 8.4 测试

- `TopologyTestDriver`：不依赖 Broker，适合 CI
- 本仓库：`RoomTemperatureTopologyTest`、`IntrusionDetectionTopologyTest`

### 8.5 未实现拓展

- Session / Sliding Window
- Stream-Table Join（KTable enrich）
- `exactly_once_v2`
- `suppress(untilWindowCloses)`
- 多实例 rebalance 演示

---

## 9. 物联网（IoT）场景

### 9.1 本仓库已覆盖的 IoT 场景

| 场景 | 模块 | Topic 示例 |
|------|------|------------|
| 设备上下线 | EventDriven | `device-status-update-topic` |
| 设备告警扇出 | FanOut | `device-alert-topic` |
| 温湿度遥测聚合 | Streams | `device.telemetry.temperature` |
| 门磁 + 人体入侵 | Streams | `device.event.door` / `device.event.motion` |
| 开放平台回调 | Outbox | `callback-demo-topic` |

### 9.2 IoT 实践要点

```
设备量大     → 分区键用 deviceId，保证单设备有序
上报频率高   → batch + linger + snappy/lz4 压缩
弱网 / 离线  → at-least-once + 幂等；重要指令配回执 Topic
多协议接入   → MQTT/CoAP/HTTP 接入层统一写 Kafka，下游只认 Kafka
```

### 9.3 待建 IoT 专题模块

| 模块 | 场景 |
|------|------|
| `Kafka-IoT-Telemetry` | 海量遥测、按 deviceId 分区 |
| `Kafka-IoT-Command` | 云端命令下发 + 回执 |
| `Kafka-IoT-Lifecycle` | 上下线 + Outbox 回调 |
| `Kafka-IoT-Alert` | 遥测流 + 规则触发（可与 Streams 联动） |

### 9.4 模式组合关系

```
Pub/Sub     + Outbox     → 开放平台 Webhook 通知 ISV
Fan-out     + DLQ        → 告警多通道 + 失败隔离
Streams     + Fan-out    → 实时计算告警 → 多下游分发
CDC         + Saga       → 设备档案同步 + 多步开通流程
```

---

## 10. 拓展知识（待建模块）

以下模式在本仓库**规划但未独立实现**，部分概念已在现有模块中有所体现。

### 10.1 Saga（长事务 + 补偿）

**问题**：跨多个服务的长事务无法用单库 ACID 保证。

**方案**：
- **编舞（Choreography）**：各服务订阅事件，自行决定下一步 / 补偿
- **编排（Orchestration）**：中央编排器驱动状态机

**推荐 IoT 学习场景**：「离家场景」联动 — 关灯 → 关窗帘 → 启用安防，任一步失败逆序补偿。

**Kafka 角色**：步骤事件总线；状态在 DB（`saga_instance` 表）。

**常搭配**：幂等表、Outbox、定时超时补偿。

### 10.2 CDC（Change Data Capture）

**问题**：数据库变更需实时同步到搜索、缓存、下游系统。

**方案**：Debezium 监听 PostgreSQL WAL → Kafka Topic。

**IoT 场景**：设备档案表变更 → `db.device.public.devices` → 同步 ES / Redis。

**与 Outbox 关系**：Outbox 是应用层 CDC 的简化版；Debezium 是基础设施层 CDC。

### 10.3 独立幂等消费模块

专注练习：
- Inbox 模式（消费前先落库去重）
- Redis / DB 多种去重策略对比
- 与 Outbox 对称设计

**可参考**：`Kafka-FanOut`（`event_id`）、`Kafka-EventSourcing`（`commandId`）

### 10.4 独立 DLQ 模块

专注练习：
- poison message 识别与隔离
- DLQ 重放工具
- 告警与人工处理流程

**可参考**：`Kafka-FanOut` 的 `DeadLetterPublishingRecoverer` + `alert_dlq_archive`

### 10.5 Schema Registry

- Avro / Protobuf 强类型消息
- 演进兼容性管理
- 本仓库目前全部使用 JSON 字符串，适合学习，生产可考虑 Schema Registry

### 10.6 Kafka 安全与多租户

| 主题 | 说明 |
|------|------|
| SASL / SSL | 认证与加密 |
| ACL | Topic 级权限 |
| 多租户 | `tenant.{id}.telemetry` Topic 前缀隔离 |

### 10.7 可观测性

| 工具 | 用途 |
|------|------|
| Consumer Lag | 消费是否跟上生产 |
| JMX / Prometheus | Broker 与客户端指标 |
| 分布式追踪 | `traceId` / `correlationId` 贯穿日志 |

本仓库各模块通过 `correlationId`、`trace_id`、`event_id` 做日志追踪练习。

---

## 11. 推荐学习路线

### 阶段一：基础（必做）

| 顺序 | 模块 | 核心知识点 |
|------|------|------------|
| 1 | **Kafka-PointToPoint** | Producer、Consumer Group、竞争消费 |
| 2 | **Kafka-LogAggregation** | 多源汇聚、Consumer 落库 |
| 3 | **Kafka-EventDriven** | 事件驱动、acks、压缩 |

### 阶段二：可靠性（必做）

| 顺序 | 模块 | 核心知识点 |
|------|------|------------|
| 4 | **Kafka-Outbox-Model** | Outbox、Relay、消费重试 |
| 5 | **Kafka-FanOut** | Fan-out、DLQ、手动 ack、幂等 |

### 阶段三：进阶（选做）

| 顺序 | 模块 | 核心知识点 |
|------|------|------------|
| 6 | **Kafka-EventSourcing** | Event Store、CQRS 投影、命令幂等 |
| 7 | **Kafka-RequestReply** | 双 Topic、correlationId、Future 配对 |
| 8 | **Kafka-Streams** | 窗口聚合、Join、State Store |

### 阶段四：拓展（待建）

| 顺序 | 模块 | 核心知识点 |
|------|------|------------|
| 9 | Kafka-Saga | 长事务、补偿 |
| 10 | Kafka-CDC | Debezium、变更同步 |
| 11 | Kafka-IoT-* | 遥测、命令、生命周期 |

### 模块依赖关系

```
PointToPoint ──► LogAggregation ──► EventDriven
                                        │
                    Outbox ◄────────────┤
                      │                 │
                    FanOut ◄────────────┤
                      │                 │
              EventSourcing ◄───────────┤
                      │                 │
              RequestReply ◄────────────┤
                      │                 │
              Kafka-Streams ◄─────────────┘
```

---

## 12. 配置与运维速查

### 12.1 本地环境启动

```bash
# 项目根目录
docker compose up -d

# 构建
mvn clean install

# 运行指定模块
mvn -pl Kafka-RequestReply spring-boot:run
```

Kafka 地址：`localhost:9092`（通过 `APP_KAFKA_BOOTSTRAP_SERVERS` 覆盖）

### 12.2 docker-compose 关键参数

| 参数 | 值 | 说明 |
|------|-----|------|
| 镜像 | `apache/kafka:3.9.2` | KRaft 单机模式 |
| 端口 | `9092` | 宿主机访问 |
| 默认分区数 | `3` | `KAFKA_NUM_PARTITIONS` |
| 自动建 Topic | `true` | 开发便利 |
| 数据目录 | `./data` | 持久化 |

### 12.3 常用 CLI

```bash
# 列出 Topic
docker exec kafka-learn /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --list

# 消费消息
docker exec kafka-learn /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic <topic-name> --from-beginning

# 查看消费组 lag
docker exec kafka-learn /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe --group <group-id>
```

### 12.4 端口规划

| 模块 | 默认端口 |
|------|----------|
| PointToPoint / LogAgg / EventDriven / EventSourcing | 8080 |
| FanOut / RequestReply | 8081 |
| Kafka-Streams | 8082 |
| Outbox-Model | 17070 |

同时运行多模块时请用 `APP_SERVER_PORT` 避免冲突。

---

## 13. 术语表

| 术语 | 英文 | 解释 |
|------|------|------|
| 主题 | Topic | 消息的逻辑分类 |
| 分区 | Partition | Topic 的物理分片，分区内有序 |
| 偏移量 | Offset | 分区内的消息序号 |
| 消费者组 | Consumer Group | 共同消费一个 Topic 的消费者集合 |
| 再均衡 | Rebalance | Group 成员变化时重新分配分区 |
| 生产者幂等 | Idempotent Producer | 重试时不重复写入 |
| 至少一次 | At-least-once | 可能重复，不丢 |
| 至多一次 | At-most-once | 可能丢，不重复 |
| 精确一次 | Exactly-once | 不丢不重（端到端难） |
| 死信队列 | DLQ | 多次失败消息的隔离 Topic |
| 发件箱 | Outbox | 与业务同事务写入的待发消息表 |
| 关联 ID | correlationId | 配对 request/reply 或追踪链路 |
| 事件溯源 | Event Sourcing | 以事件序列作为状态真相源 |
| 命令查询分离 | CQRS | 写模型与读模型分离 |
| 扇出 | Fan-out | 一条消息被多个下游独立处理 |
| 编舞 Saga | Choreography | 各服务通过事件协作的长事务 |
| 编排 Saga | Orchestration | 中央协调器驱动的长事务 |
| 变更捕获 | CDC | 捕获数据库变更并发布为事件 |
| 状态存储 | State Store | Streams 应用的本地状态（RocksDB） |
| 滚动窗口 | Tumbling Window | 固定大小、不重叠的时间窗口 |

---

## 附录：各模块 Topic 一览

| 模块 | Topic |
|------|-------|
| PointToPoint | `kafka-learn-producer` |
| LogAggregation | `kafka-log-aggregation-topic` |
| EventDriven | `device-status-update-topic` |
| Outbox | `callback-demo-topic` |
| FanOut | `device-alert-topic`、`*-dlq` |
| EventSourcing | `account-events` |
| RequestReply | `balance-query-request`、`balance-query-reply` |
| Streams | `device.telemetry.temperature`、`device.event.door`、`device.event.motion`、`streams.*` |

---

## 附录：进一步阅读

| 资源 | 说明 |
|------|------|
| 各模块 `README.md` | 运行方式、验证步骤、学到了什么 |
| [Apache Kafka 官方文档](https://kafka.apache.org/documentation/) | Broker、协议、Streams |
| [Spring for Apache Kafka 文档](https://docs.spring.io/spring-kafka/reference/) | KafkaTemplate、@KafkaListener、ErrorHandler |
| 根目录 [README.md](../README.md) | 模块索引与学习路线 |
| 根目录 [docker-compose.yml](../docker-compose.yml) | 本地 Kafka 环境 |

---

*本文档随 Kafka-Learn 仓库模块演进更新。完成新模块后请同步更新第 3、4、11 节。*
