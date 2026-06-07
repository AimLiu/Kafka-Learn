# Kafka-Learn

Kafka 组件学习与实验仓库。以 Java 21 + Spring Boot 3 为主，通过独立 Maven 模块逐步实践 Kafka 的常见使用模式，以及物联网（IoT）场景下的典型用法。

本仓库定位为**学习项目**：每个模式对应一个（或一组）可运行的 demo 模块，由学习者自行新建模块、对照本文档推进。

---

## 技术栈

| 组件 | 版本 / 说明 |
|------|-------------|
| Java | 21 |
| Spring Boot | 3.3.5 |
| Kafka | 3.7（本地 Docker，见 `docker-compose.yml`） |
| 数据库 | PostgreSQL（Outbox 等可靠性模式实验） |
| 构建 | Maven 多模块 |

---

## 快速开始

### 1. 启动 Kafka

```bash
docker compose up -d
```

详细说明见 [docs/Kafka部署说明.md](docs/Kafka部署说明.md)。默认连接地址：`localhost:9092`。

### 2. 构建项目

```bash
mvn clean install
```

### 3. 运行已有模块

| 模块 | 说明 |
|------|------|
| `Kafka-Producer` | 基础生产者：定时发送、回调消息结构 |
| `Kafka-Outbox-Model` | Outbox + 第三方回调通知解耦（含重试、补偿） |

各模块运行方式见其目录下的 Spring Boot 主类与 `application.yml`。

---

## 项目结构

```
Kafka-Learn/
├── pom.xml                    # 父 POM，统一管理依赖
├── docker-compose.yml         # 本地 Kafka 环境
├── README.md                  # 本文件：学习路线与模式索引
├── docs/                      # 设计文档、部署说明
├── Kafka-Producer/            # 【已有】基础生产者
├── Kafka-Outbox-Model/        # 【已有】Outbox + 回调解耦
└── Kafka-xxx/                 # 【待建】按下方学习路线自行新建模块
```

新建模块时，在根 `pom.xml` 的 `<modules>` 中注册，并继承父 POM。

---

## 已有模块说明

### Kafka-Producer

理解 Kafka 生产者最基本的能力：配置、发送、序列化、定时调度。

### Kafka-Outbox-Model

实现 **Transactional Outbox + 异步回调通知** 最小可靠链路：

```
客户端 → 写 callback_task + callback_outbox（同事务）
      → Outbox Relay 发 Kafka
      → Consumer 消费 → 模拟通知第三方
      → 回写任务状态与消费日志
      → 调度器补偿 / 重试
```

相关文档：

- [回调解耦学习版-需求文档.md](docs/回调解耦学习版-需求文档.md)
- [回调解耦学习版-设计文档.md](docs/回调解耦学习版-设计文档.md)
- [第三方回调通知解耦增强-详细设计文档.md](docs/第三方回调通知解耦增强-详细设计文档.md)

---

## Kafka 常用使用模式

以下模式均可作为独立学习模块的目标。建议按「基础 → 可靠性 → 进阶」顺序推进。

### 模式总览

| # | 模式 | 核心思想 | 典型场景 | 建议模块名（示例） |
|---|------|----------|----------|-------------------|
| 1 | 点对点（Queue-like） | 同一 Consumer Group 内，每条消息只被一个实例消费 | 任务分发、异步作业 | `Kafka-PointToPoint` |
| 2 | 发布/订阅（Pub/Sub） | 不同 Consumer Group 独立消费同一 Topic | 一事件多下游 | `Kafka-PubSub` |
| 3 | 日志聚合（Log Aggregation） | 多源写入 Kafka，下游统一消费 | 应用日志、监控指标 | `Kafka-LogAggregation` |
| 4 | 事件驱动（Event-Driven） | 状态变更以事件发布，服务解耦 | 微服务通信 | `Kafka-EventDriven` |
| 5 | 事件溯源（Event Sourcing） | 状态变更序列存为事件流，可重放 | 账户流水、审计 | `Kafka-EventSourcing` |
| 6 | CQRS | 写模型发事件，读模型异步投影 | 读写分离 | `Kafka-CQRS` |
| 7 | 扇出（Fan-out） | 一条消息被多个下游并行处理 | 订单触发多系统 | `Kafka-FanOut` |
| 8 | 请求-响应（Request-Reply） | 请求 Topic + 响应 Topic + correlationId | 跨服务 RPC 式调用 | `Kafka-RequestReply` |
| 9 | 竞争消费者（Competing Consumers） | 同 Group 多实例水平扩展吞吐 | 高并发消费 | `Kafka-CompetingConsumers` |
| 10 | 幂等消费（Idempotent Consumer） | 业务键去重，应对 at-least-once 重复 | 支付、回调 | `Kafka-IdempotentConsumer` |
| 11 | 死信队列（DLQ） | 多次失败后转入 DLQ | Poison message 隔离 | `Kafka-DLQ` |
| 12 | CDC（Change Data Capture） | DB 变更经 Debezium 等写入 Kafka | 数据库变更同步 | `Kafka-CDC` |
| 13 | Saga（编排 / 编舞） | 长事务拆成多步事件 + 补偿 | 分布式事务 | `Kafka-Saga` |
| 14 | 流处理（Kafka Streams） | 流式聚合、窗口、Join | 实时统计、风控 | `Kafka-Streams` |

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

IoT 场景大多仍是 Pub/Sub、分区有序、至少一次投递与 Outbox/幂等的组合，但数据特征（海量、时序、弱网）不同。

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

可按难度分阶段自建模块；已有模块可直接对照源码。

### 阶段一：基础（必做）

1. **Kafka-Producer**（已有）— 生产者 API、配置、发送
2. **Kafka-PubSub**（待建）— 一个 Topic、多个 Consumer Group
3. **Kafka-PointToPoint**（待建）— 同 Group 多实例竞争消费

### 阶段二：可靠性（必做）

4. **Kafka-Outbox-Model**（已有）— Outbox + 回调通知
5. **Kafka-IdempotentConsumer**（待建）— 消费端去重
6. **Kafka-DLQ**（待建）— 失败消息隔离与人工处理

### 阶段三：进阶（选做）

7. **Kafka-RequestReply** —  correlationId 请求响应
8. **Kafka-Streams** — 窗口聚合、简单 Join
9. **Kafka-CDC** — Debezium + PostgreSQL
10. **Kafka-Saga** — 多步事件 + 补偿

### 阶段四：IoT 专题（选做）

11. **Kafka-IoT-Telemetry** — 模拟海量设备上报、按 deviceId 分区
12. **Kafka-IoT-Command** — 云端命令下发、回执 Topic
13. **Kafka-IoT-Lifecycle** — 上下线事件 + Outbox 回调（可与 Outbox 模块联动）
14. **Kafka-IoT-Alert** — 遥测流 + 简单规则触发告警

每完成一个模块，建议在模块目录下补充简短 `README.md`，记录：Topic 设计、关键配置、如何运行、学到了什么。

---

## 新建模块 checklist

1. 在根目录创建 `Kafka-xxx/` 子模块
2. 在根 `pom.xml` 的 `<modules>` 中注册
3. 子模块 `pom.xml` 继承 `kafka-learn` 父 POM
4. 配置 `application.yml` 中的 `spring.kafka.bootstrap-servers: localhost:9092`
5. 为本模式定义清晰的 Topic 命名与消息结构
6. 编写可重复的本地验证步骤（curl、测试类或脚本）

---

## 相关文档

| 文档 | 说明 |
|------|------|
| [docs/Kafka部署说明.md](docs/Kafka部署说明.md) | Docker 启动 Kafka |
| [docs/回调解耦学习版-需求文档.md](docs/回调解耦学习版-需求文档.md) | Outbox 学习版需求 |
| [docs/回调解耦学习版-设计文档.md](docs/回调解耦学习版-设计文档.md) | Outbox 学习版设计与实现对照 |
| [docs/第三方回调通知解耦增强-详细设计文档.md](docs/第三方回调通知解耦增强-详细设计文档.md) | 生产化扩展参考 |

---

## 许可证与说明

本项目仅供个人学习使用，示例代码与配置不适用于生产环境。
