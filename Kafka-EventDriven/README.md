# Kafka-EventDriven

事件驱动模式学习模块：设备上下线状态变更以事件形式发布，下游异步消费并更新 PostgreSQL 中的设备当前状态。

## 设计思想

强调「状态变更 = 事件」而非直接跨服务改库：

- **发布方只发事件**，不关心有多少下游、如何存储。
- **消费方订阅事件**，自行更新本地 `device_status` 表（模拟下游服务）。
- **真相在消费侧当前状态表**，Kafka 是传递通道，不保留完整历史。

```
DeviceStatusSimulator（定时模拟上下线）
    → EventPublisher 发布 DeviceStatusChangedEvent
    → Kafka Topic: device-status-update-topic
    → DeviceStatusEventConsumer 消费并更新 device_status 表
```

与 EventSourcing 对比：本模块只维护**最新状态**，不做事件重放。

## 用到的 Kafka 特性

| 特性 | 本模块用法 |
|------|------------|
| Producer | `KafkaTemplate` + `StringSerializer`，JSON 字符串 payload |
| Consumer | `@KafkaListener`，`device-status-event-group` |
| Topic | `device-status-update-topic` |
| 可靠性 | Producer `acks=all`、`retries=3` |
| 批处理 | `batch-size`、`linger-ms`、`snappy` 压缩 |
| offset | `auto-offset-reset: earliest`；`enable-auto-commit: true` |
| 编解码 | `DeviceStatusChangedEventCodec` 自定义 JSON |

**未涉及**：Outbox、DLQ、手动 ack、事件溯源、Fan-out。

## 运行

```bash
docker compose up -d

mvn clean install
cd Kafka-EventDriven
mvn spring-boot:run
```

主类：`com.kafkalearn.KafkaEventDrivenApplication`

## 关键配置

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `APP_KAFKA_BOOTSTRAP_SERVERS` | `192.168.19.64:9092` | Kafka 地址 |
| `APP_KAFKA_CONSUMER_GROUP` | `device-status-event-group` | 消费者组 |
| `APP_DB_HOST` / `APP_DB_NAME` | `192.168.19.64` / `kafka-learn` | PostgreSQL |
| `APP_SERVER_PORT` | `8080` | HTTP 端口 |

Topic 常量：`com.kafkalearn.config.KafkaTopic#statusUpdateTopic`

## 数据库建表

以下 SQL 与 `DeviceStatus` 实体一致。`alive` 列存储 `ActiveStatus` 枚举字符串（`ACTIVE` / `DISACTIVE`）。

```sql
create table if not exists device_status (
    id                uuid primary key,
    device_id         uuid         not null,
    alive             varchar(32)  not null,  -- ACTIVE | DISACTIVE
    last_connect_time timestamp    not null default current_timestamp
);
```

也可使用 JPA `ddl-auto: update` 自动建表。

## Entity 对照

| 表名 | Entity 类 | 字段映射 |
|------|-----------|----------|
| `device_status` | `DeviceStatus` | Java 字段 `active` → 列 `alive` |

## 学到了什么

- 事件驱动如何实现服务解耦：发布方与消费方独立演进。
- Kafka 在 IoT 设备生命周期场景中的典型用法。
- 与 EventSourcing 的边界：当前状态表 vs 事件流真相源。
