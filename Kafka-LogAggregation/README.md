# Kafka-LogAggregation

日志聚合模式学习模块：多源应用日志经 Kafka 汇聚到同一 Topic，由统一 Consumer 消费并持久化到 PostgreSQL。

## 设计思想

模拟「多服务 → 中央日志管道 → 统一存储」的典型可观测性架构：

- **多源写入**：`SheduleProducer` 定时模拟不同来源的日志消息。
- **单 Topic 汇聚**：所有日志进入 `kafka-log-aggregation-topic`。
- **统一消费落库**：`LogConsumer` 解析后写入 `aggregation_log`，便于后续检索与分析。

```
SheduleProducer（定时模拟多源）
    → KafkaMsgProducer 发送 KafkaSendMsg
    → Topic: kafka-log-aggregation-topic
    → LogConsumer 消费并写入 aggregation_log 表
```

## 用到的 Kafka 特性

| 特性 | 本模块用法 |
|------|------------|
| Producer | `KafkaTemplate` + `JsonSerializer` 发送 `KafkaSendMsg` |
| Consumer | `@KafkaListener` + `JsonDeserializer` |
| Topic | 单 Topic 聚合 |
| Consumer Group | `log-aggregation-group` |
| 序列化 | Key: String；Value: JSON（`KafkaSendMsg`） |
| 压缩 | Producer `gzip`（可配置） |
| 幂等键 | 消息体 `msg_id`（UUID），对应表主键 |

**未涉及**：Kafka Streams 聚合、按服务分 Topic、DLQ、手动 ack。

## 运行

```bash
docker compose up -d

mvn clean install   # 项目根目录
cd Kafka-LogAggregation
mvn spring-boot:run
```

主类：`com.kafkalearn.KafkaLogAggregationApplication`

## 关键配置

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `APP_KAFKA_PRODUCER_BOOTSTRAP_SERVERS` | `192.168.19.64:9092` | Producer Kafka |
| `APP_KAFKA_CONSUMER_BOOTSTRAP_SERVERS` | `192.168.19.64:9092` | Consumer Kafka |
| `APP_KAFKA_CONSUMER_GROUP` | `log-aggregation-group` | 消费者组 |
| `APP_KAFKA_PRODUCE_TOPIC` | `kafka-log-aggregation-topic` | 日志 Topic |
| `APP_DB_HOST` / `APP_DB_NAME` | `192.168.19.64` / `kafka-learn` | PostgreSQL |
| `app.scheduler.send-delay-ms` | `5000` | 模拟发送间隔 |

## 数据库建表

以下 SQL 与 `LogEntity` 字段对齐（已修正列长度：`log_from` 64、`msg` 512）。

```sql
create table if not exists aggregation_log (
    msg_id    uuid primary key,
    log_from  varchar(64)  not null,
    msg       varchar(512) not null,
    succ      boolean      not null,
    log_time  timestamp    not null default current_timestamp
);
```

也可使用 JPA `ddl-auto: update` 自动建表。

## Entity 对照

| 表名 | Entity 类 | 说明 |
|------|-----------|------|
| `aggregation_log` | `LogEntity` | `log_from` 映射字段 `from`；`succ` 映射 `boolean succ` |

## 学到了什么

- 日志聚合是 Pub/Sub 的特化：多写者、单 Topic、统一消费。
- JSON 序列化在 Spring Kafka 中的 Producer/Consumer 配置方式。
- 以 `msg_id` 作为主键实现消费侧幂等的基础思路。
