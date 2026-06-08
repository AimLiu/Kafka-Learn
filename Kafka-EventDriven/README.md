# Kafka-EventDriven

事件驱动模式学习模块：模拟设备上下线事件，经 Kafka 异步通知下游，并更新 PostgreSQL 中的设备状态。

## 流程

```
DeviceStatusSimulator（定时模拟）
    → EventPublisher 发布 DeviceStatusChangedEvent
    → Kafka Topic: device-status-update-topic
    → DeviceStatusEventConsumer 消费并更新 device_status 表
```

## 运行

```bash
# 在项目根目录
mvn clean install

# 启动本模块
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

Topic 名称见 `com.kafkalearn.config.KafkaTopic#statusUpdateTopic`。

## 数据库建表

启动前请在 PostgreSQL 中执行以下 SQL（也可配合 JPA `ddl-auto: update` 使用）：

```sql
create table if not exists device_status(
    id                uuid primary key,
    device_id         uuid        not null,
    alive             varchar(32) not null,
    last_connect_time timestamp   not null default current_timestamp
);
```

## 说明

- 事件体在发送前通过 `DeviceStatusChangedEventCodec` 序列化为 JSON 字符串，Kafka 使用 `StringSerializer`。
- `alive` 字段存储设备活跃状态（如 `ACTIVE` / `DISACTIVE`），与实体 `DeviceStatus.active` 对应。
