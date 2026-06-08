# Kafka-LogAggregation

日志聚合模式学习模块：定时模拟多源日志消息写入 Kafka，Consumer 统一消费并持久化到 PostgreSQL 的 `aggregation_log` 表。

## 流程

```
SheduleProducer（定时模拟）
    → KafkaMsgProducer 发送 KafkaSendMsg
    → Kafka Topic: kafka-log-aggregation-topic
    → LogConsumer 消费并写入 aggregation_log 表
```

## 运行

```bash
# 在项目根目录
mvn clean install

# 启动本模块
cd Kafka-LogAggregation
mvn spring-boot:run
```

主类：`com.kafkalearn.KafkaLogAggregationApplication`

## 关键配置

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `APP_KAFKA_PRODUCER_BOOTSTRAP_SERVERS` | `192.168.19.64:9092` | Producer Kafka 地址 |
| `APP_KAFKA_CONSUMER_BOOTSTRAP_SERVERS` | `192.168.19.64:9092` | Consumer Kafka 地址 |
| `APP_KAFKA_CONSUMER_GROUP` | `log-aggregation-group` | 消费者组 |
| `APP_KAFKA_PRODUCE_TOPIC` | `kafka-log-aggregation-topic` | 日志 Topic |
| `APP_DB_HOST` / `APP_DB_NAME` | `192.168.19.64` / `kafka-learn` | PostgreSQL |

## 数据库建表

启动前请在 PostgreSQL 中执行以下 SQL（也可配合 JPA `ddl-auto: update` 使用）：

```sql
create table if not exists aggregation_log
(
    msg_id      uuid primary key,
    log_from    varchar(128) not null,
    msg         varchar(128) not null,
    succ        bool         not null,
    log_time    timestamp    not null default current_timestamp
);
```

## 说明

- Producer / Consumer 使用 Spring Kafka 的 `JsonSerializer` / `JsonDeserializer` 处理 `KafkaSendMsg`。
- 消息幂等键为 `msg_id`（UUID）。
