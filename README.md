# Kafka-Learn

Kafka 组件学习与实验仓库。以 Java 21 + Spring Boot 3 为主，通过独立 Maven 模块逐步实践 Kafka 的常见使用模式，以及物联网（IoT）场景下的典型用法。

本仓库定位为**学习项目**：每个模式对应一个可运行的 demo 模块，模块目录下均有 `README.md` 记录设计、配置与验证步骤。

---

## 技术栈

| 组件 | 版本 / 说明 |
|------|-------------|
| Java | 21 |
| Spring Boot | 3.3.5 |
| Kafka | 3.7（本地 Docker，见 `docker-compose.yml`） |
| 数据库 | PostgreSQL（Outbox、EventSourcing、FanOut 等模块） |
| 构建 | Maven 多模块 |

---

## 快速开始

### 1. 启动 Kafka

```bash
docker compose up -d
```

本地 Kafka 由根目录 `docker-compose.yml` 提供。默认连接地址：`localhost:9092`（各模块 `application.yml` 可通过环境变量覆盖）。

### 2. 构建项目

```bash
mvn clean install
```

### 3. 运行模块

```bash
# 示例：运行 Request-Reply 模块
mvn -pl Kafka-RequestReply spring-boot:run
```

或在模块目录下：

```bash
cd Kafka-RequestReply
mvn spring-boot:run
```

---

## 已有模块索引

根 `pom.xml` 当前注册 **7 个子模块**，均已实现并可运行。详情见各模块 `README.md`。

| 模块 | 对应模式 | 默认端口 | 主类 | 一句话说明 |
|------|----------|----------|------|------------|
| [Kafka-PointToPoint](Kafka-PointToPoint/) | 点对点 / 竞争消费 | 8080 | `KafkaProducerApplication` | 单 Topic、同 Group 多监听器竞争消费，理解 Queue-like 语义 |
| [Kafka-LogAggregation](Kafka-LogAggregation/) | 日志聚合 | 8080 | `KafkaLogAggregationApplication` | 多源日志写入同一 Topic，统一 Consumer 落库 PostgreSQL |
| [Kafka-EventDriven](Kafka-EventDriven/) | 事件驱动 | 8080 | `KafkaEventDrivenApplication` | 设备上下线事件发布，下游异步更新当前状态表 |
| [Kafka-Outbox-Model](Kafka-Outbox-Model/) | Outbox + 回调解耦 | 17070 | `KafkaLearnApplication` | 同事务写业务表 + Outbox，Relay 发 Kafka，Consumer 模拟第三方回调 |
| [Kafka-FanOut](Kafka-FanOut/) | 扇出 + DLQ + 幂等 | 8081 | `KafkaFanOutApplication` | 一条告警被三个 Consumer Group 并行处理，失败进 DLQ 并归档 |
| [Kafka-EventSourcing](Kafka-EventSourcing/) | 事件溯源 + CQRS 投影 | 8080 | `KafkaEventSourcingApplication` | 银行账户命令追加 `es_event`，Kafka 驱动读侧异步投影 |
| [Kafka-RequestReply](Kafka-RequestReply/) | 请求-响应 | 8081 | `KafkaRequestReplyApplication` | request/reply 双 Topic + `correlationId` 配对，HTTP 同步等待 reply |

> **端口冲突提示**：多个模块默认使用 `8080` 或 `8081`，同时运行时请通过 `APP_SERVER_PORT` 指定不同端口。

---

## 项目结构

```
Kafka-Learn/
├── pom.xml                         # 父 POM，统一管理依赖与模块
├── docker-compose.yml              # 本地 Kafka 环境
├── README.md                       # 本文件：模块索引与学习路线
├── Kafka-PointToPoint/             # 【已有】点对点 / 竞争消费
├── Kafka-LogAggregation/           # 【已有】日志聚合
├── Kafka-EventDriven/              # 【已有】事件驱动
├── Kafka-Outbox-Model/             # 【已有】Transactional Outbox + 回调
├── Kafka-FanOut/                   # 【已有】扇出 + DLQ + 幂等消费
├── Kafka-EventSourcing/            # 【已有】事件溯源 + 读模型投影
├── Kafka-RequestReply/             # 【已有】Request-Reply + correlationId
└── Kafka-xxx/                      # 【待建】见下方学习路线
```

新建模块时，在根 `pom.xml` 的 `<modules>` 中注册，并继承父 POM。

---

## 已有模块说明（摘要）

### Kafka-PointToPoint

最简「生产 → 消费」链路：定时 Producer 发送消息，同 Consumer Group 内多个监听器竞争分区。无数据库、无复杂业务，适合作为 Kafka 入门第一步。

→ [Kafka-PointToPoint/README.md](Kafka-PointToPoint/README.md)

### Kafka-LogAggregation

多源应用日志汇聚到单一 Topic，由统一 Consumer 解析并写入 `aggregation_log` 表，模拟可观测性管道。

→ [Kafka-LogAggregation/README.md](Kafka-LogAggregation/README.md)

### Kafka-EventDriven

强调「状态变更 = 事件」：发布方只发 `DeviceStatusChangedEvent`，消费方自行维护 `device_status` 当前状态表（非完整历史）。

→ [Kafka-EventDriven/README.md](Kafka-EventDriven/README.md)

### Kafka-Outbox-Model

Transactional Outbox + 异步回调通知最小可靠链路：

```
客户端 → 写 callback_task + callback_outbox（同事务）
      → Outbox Relay 发 Kafka
      → Consumer 消费 → 模拟通知第三方
      → 回写任务状态与消费日志
      → 调度器补偿 / 重试
```


→ [Kafka-Outbox-Model/README.md](Kafka-Outbox-Model/README.md)

### Kafka-FanOut

一条告警事件被存储、推送、规则引擎三个 Consumer Group 独立并行处理；`DefaultErrorHandler` + `DeadLetterPublishingRecoverer` 将 poison message 转入 DLQ 并归档；各下游按 `event_id` 幂等去重。

→ [Kafka-FanOut/README.md](Kafka-FanOut/README.md)

### Kafka-EventSourcing

银行账户场景：命令不直接 UPDATE 余额，而是向 `es_event` 追加不可变事件；Kafka 分发事件驱动 `AccountEventProjector` 更新读模型视图。

→ [Kafka-EventSourcing/README.md](Kafka-EventSourcing/README.md)

### Kafka-RequestReply

余额查询场景：调用方发 request 并注册 `CompletableFuture`，处理方消费 request 后发 reply，`ReplyListener` 按 `correlationId` 完成 Future。演示 Kafka 上实现应用层 Request-Reply 协议（非 Kafka 内置 RPC）。

→ [Kafka-RequestReply/README.md](Kafka-RequestReply/README.md)

---

## Kafka 常用使用模式

以下模式均可作为独立学习模块的目标。表中 **状态** 列标注本仓库当前实现情况。

### 模式总览

| # | 模式 | 核心思想 | 典型场景 | 模块 | 状态 |
|---|------|----------|----------|------|------|
| 1 | 点对点（Queue-like） | 同一 Consumer Group 内，每条消息只被一个实例消费 | 任务分发、异步作业 | `Kafka-PointToPoint` | **已有** |
| 2 | 发布/订阅（Pub/Sub） | 不同 Consumer Group 独立消费同一 Topic | 一事件多下游 | `Kafka-PubSub` | 待建（可参考 `Kafka-FanOut`、`Kafka-EventDriven`） |
| 3 | 日志聚合（Log Aggregation） | 多源写入 Kafka，下游统一消费 | 应用日志、监控指标 | `Kafka-LogAggregation` | **已有** |
| 4 | 事件驱动（Event-Driven） | 状态变更以事件发布，服务解耦 | 微服务通信 | `Kafka-EventDriven` | **已有** |
| 5 | 事件溯源（Event Sourcing） | 状态变更序列存为事件流，可重放 | 账户流水、审计 | `Kafka-EventSourcing` | **已有** |
| 6 | CQRS | 写模型发事件，读模型异步投影 | 读写分离 | `Kafka-EventSourcing`（读模型投影） | **已有（合并在 ES 模块）** |
| 7 | 扇出（Fan-out） | 一条消息被多个下游并行处理 | 订单触发多系统 | `Kafka-FanOut` | **已有** |
| 8 | 请求-响应（Request-Reply） | 请求 Topic + 响应 Topic + correlationId | 跨服务 RPC 式调用 | `Kafka-RequestReply` | **已有** |
| 9 | 竞争消费者（Competing Consumers） | 同 Group 多实例水平扩展吞吐 | 高并发消费 | `Kafka-PointToPoint` | **已有（合并在点对点模块）** |
| 10 | 幂等消费（Idempotent Consumer） | 业务键去重，应对 at-least-once 重复 | 支付、回调 | `Kafka-IdempotentConsumer` | 待建（`Kafka-FanOut` 已演示 `event_id` 去重） |
| 11 | 死信队列（DLQ） | 多次失败后转入 DLQ | Poison message 隔离 | `Kafka-DLQ` | 待建（`Kafka-FanOut` 已演示 DLQ + 归档） |
| 12 | CDC（Change Data Capture） | DB 变更经 Debezium 等写入 Kafka | 数据库变更同步 | `Kafka-CDC` | 待建 |
| 13 | Saga（编排 / 编舞） | 长事务拆成多步事件 + 补偿 | 分布式事务 | `Kafka-Saga` | 待建 |
| 14 | 流处理（Kafka Streams） | 流式聚合、窗口、Join | 实时统计、风控 | `Kafka-Streams` | 待建 |

### Outbox + 接收消息回调 + 通知第三方平台

**状态：已在 `Kafka-Outbox-Model` 中实现（学习版）。**

#### 要解决的问题

业务写库成功，但同步发 Kafka 或 HTTP 回调失败，会出现「库里有记录、外部没收到通知」的不一致。

#### 标准流程

```
1. 客户端创建任务
2. Service 在同一 DB 事务中写入业务表 + outbox 表
3. Outbox Relay（定时或 CDC）读取待发送记录，发布到 Kafka
4. Consumer 消费消息，调用第三方 Webhook / 开放平台回调
5. 回写任务状态、消费日志；失败则进入重试 / 补偿调度
```

#### 关键设计点

| 要点 | 说明 |
|------|------|
| 事实源 | 数据库是最终真相，Kafka 只是异步传输层 |
| 同事务写 Outbox | 保证「有业务记录必有待发消息」 |
| Relay 补偿 | 扫描 `PENDING` outbox，应对 Kafka 短暂不可用 |
| At-least-once | Kafka 可能重复投递，Consumer 必须幂等 |
| 消费失败重试 | 状态机 + 调度器重新入队或重发 |
| Inbox（可选扩展） | 消费端落库去重，与 Outbox 对称 |

#### 与其他模式的关系

Outbox 常配合 **幂等消费**、**DLQ**、**Saga 补偿** 使用；在 IoT 开放平台中，设备上下线、属性变更等事件也常走此链路通知 ISV。

---

## 物联网（IoT）中 Kafka 的常用模式

IoT 场景大多仍是 Pub/Sub、分区有序、至少一次投递与 Outbox/幂等的组合，但数据特征（海量、时序、弱网）不同。本仓库中 **EventDriven**、**FanOut** 模块已用设备上下线 / 告警场景做简化演示。

### 模式总览

| # | 模式 | 数据流向 | Topic 划分示例 | 说明 | 建议模块名（示例） |
|---|------|----------|----------------|------|-------------------|
| 1 | 设备遥测上报（Telemetry） | 设备 → 网关/云 → Kafka → 存储/分析 | `device.telemetry.{productId}` | 海量时序；按 deviceId 分区保证单设备有序 | `Kafka-IoT-Telemetry` |
| 2 | 设备命令下发（Command） | 云 → Kafka → 接入层 → 设备 | `device.command.{deviceId}` | 远程控制、配置下发 | `Kafka-IoT-Command` |
| 3 | 设备影子 / 状态同步（Shadow） | 上报 + 期望状态 → Kafka → 影子服务 | `device.shadow.reported` / `desired` | 离线设备上线后对齐状态 | `Kafka-IoT-Shadow` |
| 4 | 在线 / 离线生命周期 | 连接层 → Kafka → 业务 | `device.lifecycle` | 注册、上线、下线、鉴权失败 | `Kafka-IoT-Lifecycle` |
| 5 | 规则引擎 / 告警管道 | 遥测流 → 流处理 → 告警 Topic | `telemetry` → `alert.triggered` | 阈值、组合规则 | `Kafka-IoT-Alert` |
| 6 | 多租户隔离 | 按 tenant/product 分 Topic 或前缀 | `tenant.{id}.telemetry` | 配额、ACL | `Kafka-IoT-MultiTenant` |
| 7 | 边缘聚合（Edge Aggregation） | 边缘网关聚合 → 批量上报 | 边缘 Topic → 云端 Topic | 降带宽、预处理 | `Kafka-IoT-Edge` |
| 8 | OTA 固件升级通知 | 管理平台 → Kafka → 设备接入 | `device.ota.notify` | 广播或按批次推送 | `Kafka-IoT-OTA` |
| 9 | 地理围栏 / 位置事件 | 位置流 → 窗口计算 → 事件 Topic | `location` → `geofence.enter/exit` | 流式窗口、Join | `Kafka-IoT-Geofence` |
| 10 | 数字孪生 / 实时大屏 | 多源事件 → Kafka → 孪生服务 | 多 Topic 订阅 | 设备、环境、业务事件融合 | `Kafka-IoT-DigitalTwin` |
| 11 | Outbox + 开放平台回调 | 设备事件 → DB + Outbox → Kafka → 开发者 URL | 见 `Kafka-Outbox-Model` | 属性变更、上下线通知 ISV | 扩展 Outbox 模块 |
| 12 | CDC + 设备档案同步 | 设备 DB 变更 → Debezium → Kafka | `db.device.public.devices` | 元数据变更同步搜索/缓存 | `Kafka-IoT-CDC` |

### IoT 场景下的实践要点

```
设备量级大   →  分区按 deviceId 哈希，保证单设备消息有序
上报频率高   →  批量发送、压缩（lz4/snappy）、适当 batch.size
弱网 / 离线  →  至少一次 + 幂等；重要指令可配合 ack 回执 Topic
多协议接入   →  MQTT/CoAP/HTTP 接入层统一写入 Kafka，下游只认 Kafka
```

### 模式关系简图

```
通用模式                    可靠性                     IoT 典型组合
─────────                  ────────                   ─────────────
Pub/Sub          ──►       Outbox          ──►       开放平台 Webhook
点对点           ──►       Inbox 幂等      ──►       命令下发
Fan-out          ──►       DLQ             ──►       遥测 → 多下游
Kafka Streams    ──►       重试 / 补偿     ──►       规则告警、围栏
CDC              ──►       Saga            ──►       设备档案同步
```

---

## 推荐学习路线

可按难度分阶段推进；**粗体**为仓库中已有模块，可直接对照源码与模块 `README.md`。

### 阶段一：基础（必做）

1. **Kafka-PointToPoint** — 生产者 API、Topic、竞争消费
2. **Kafka-LogAggregation** — 多源汇聚、Consumer 落库
3. **Kafka-EventDriven** — 事件驱动、发布与订阅解耦
4. `Kafka-PubSub`（待建）— 可与 FanOut 对比：单一 Topic、多 Group 的最小 Pub/Sub

### 阶段二：可靠性（必做）

5. **Kafka-Outbox-Model** — Outbox + 回调通知 + 补偿
6. **Kafka-FanOut** — 扇出、DLQ、幂等消费（综合演示）
7. `Kafka-IdempotentConsumer`（待建）— 专注消费端去重模式
8. `Kafka-DLQ`（待建）— 专注 DLQ 隔离与人工处理流程

### 阶段三：进阶（选做）

9. **Kafka-EventSourcing** — 事件溯源、命令/查询分离、投影
10. **Kafka-RequestReply** — correlationId、双 Topic、应用层 RPC
11. `Kafka-Streams` — 窗口聚合、简单 Join
12. `Kafka-CDC` — Debezium + PostgreSQL
13. `Kafka-Saga` — 多步事件 + 补偿

### 阶段四：IoT 专题（选做）

14. `Kafka-IoT-Telemetry` — 模拟海量设备上报、按 deviceId 分区
15. `Kafka-IoT-Command` — 云端命令下发、回执 Topic
16. `Kafka-IoT-Lifecycle` — 上下线事件 + Outbox 回调（可与 Outbox 模块联动）
17. `Kafka-IoT-Alert` — 遥测流 + 简单规则触发告警

每完成一个模块，在模块目录下维护 `README.md`：Topic 设计、关键配置、如何运行、学到了什么。

---

## 新建模块 checklist

1. 在根目录创建 `Kafka-xxx/` 子模块
2. 在根 `pom.xml` 的 `<modules>` 中注册
3. 子模块 `pom.xml` 继承 `kafka-learn` 父 POM
4. 配置 `application.yml` 中的 `spring.kafka.bootstrap-servers`（或通过 `APP_KAFKA_BOOTSTRAP_SERVERS` 覆盖）
5. 为本模式定义清晰的 Topic 命名与消息结构
6. 编写可重复的本地验证步骤（curl、测试类或脚本）
7. 补充模块 `README.md` 并更新本文件的「已有模块索引」

---

## 相关文档

| 文档 | 说明 |
|------|------|
| [Kafka-PointToPoint/README.md](Kafka-PointToPoint/README.md) | 点对点 / 竞争消费 |
| [Kafka-LogAggregation/README.md](Kafka-LogAggregation/README.md) | 日志聚合 |
| [Kafka-EventDriven/README.md](Kafka-EventDriven/README.md) | 事件驱动 |
| [Kafka-Outbox-Model/README.md](Kafka-Outbox-Model/README.md) | Outbox + 回调解耦 |
| [Kafka-FanOut/README.md](Kafka-FanOut/README.md) | 扇出 + DLQ + 幂等消费 |
| [Kafka-EventSourcing/README.md](Kafka-EventSourcing/README.md) | 事件溯源 + 读模型投影 |
| [Kafka-RequestReply/README.md](Kafka-RequestReply/README.md) | Request-Reply + correlationId |

本地 Kafka 环境见根目录 [docker-compose.yml](docker-compose.yml)。

---

## 许可证与说明

本项目仅供个人学习使用，示例代码与配置不适用于生产环境。
