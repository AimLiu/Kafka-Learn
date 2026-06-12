# Kafka-Outbox-Model

Transactional Outbox + 异步回调通知学习模块：在数据库事务内写入业务任务与 Outbox 记录，由 Relay 异步投递 Kafka，Consumer 模拟通知第三方并支持重试补偿。

## 设计思想

解决的核心问题：**业务写库成功，但同步发 Kafka 失败**，导致「库里有记录、外部没收到通知」的不一致。

```
客户端 API
    → 同事务写 callback_task + callback_outbox
    → Outbox Relay（定时）扫描 PENDING 记录发 Kafka
    → CallbackConsumer 消费 → 模拟 HTTP 回调第三方
    → 回写任务状态 + callback_consume_log
    → 失败进入重试 / 补偿调度
```

关键原则：

- **PostgreSQL 是事实源**，Kafka 只是异步传输层。
- **Outbox 与业务表同事务写入**，保证「有任务必有待发消息」。
- **At-least-once 投递**，Consumer 需结合任务状态与日志做幂等判断。

## 用到的 Kafka 特性

| 特性 | 本模块用法 |
|------|------------|
| Producer | `KafkaTemplate` 发送 `CallbackEventMessage`，`acks` 可配置 |
| Consumer | `@KafkaListener` 消费 `callback-demo-topic` |
| 序列化 | Key: `StringSerializer`；Value: `JsonSerializer` / `JsonDeserializer` |
| Consumer Group | `callback-demo-group` |
| offset 策略 | `auto-offset-reset: earliest` |
| 消息 Key | Outbox 记录中的 `message_key`（通常为 taskId） |
| 与 DB 协同 | Outbox Relay 补偿 Kafka 短暂不可用；非 Kafka 事务 |

**未涉及**：Kafka 事务性 Producer、EOS、Schema Registry、DLQ Topic。

## 运行

```bash
docker compose up -d   # Kafka
# PostgreSQL 需提前建表（见下方 SQL）

cd Kafka-Outbox-Model
mvn spring-boot:run
```

主类：`com.kafkalearn.demo.KafkaLearnApplication`  
默认端口：`17070`

## 关键配置

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `APP_KAFKA_PRODUCER_BOOTSTRAP_SERVERS` | `192.168.19.64:9092` | Producer Kafka |
| `APP_KAFKA_CONSUMER_BOOTSTRAP_SERVERS` | `192.168.19.64:9092` | Consumer Kafka |
| `APP_KAFKA_CALLBACK_TOPIC` | `callback-demo-topic` | 回调 Topic |
| `APP_KAFKA_CONSUMER_GROUP` | `callback-demo-group` | 消费者组 |
| `APP_DB_HOST` / `APP_DB_NAME` | `192.168.19.64` / `kafka-learn` | PostgreSQL |
| `app.scheduler.outbox-delay-ms` | `5000` | Outbox 扫描间隔 |

`spring.jpa.hibernate.ddl-auto: none`，需手工执行建表 SQL。

## 数据库建表

以下 SQL 与 Entity 类字段对齐（`CallbackTaskEntity`、`CallbackOutboxEntity`、`CallbackConsumeLogEntity`）。

```sql
-- 1. 回调任务（业务表）
create table if not exists callback_task (
    id               uuid primary key,
    biz_no           varchar(64)  not null,
    topic_name       varchar(128) not null,
    payload          jsonb        not null,
    simulate_mode    varchar(16)  not null,  -- SUCCESS | FAIL | RANDOM
    status           varchar(32)  not null,  -- CREATED | PUBLISHED | CONSUMING | SUCCESS | RETRY_WAITING | FAILED
    trace_id         varchar(64)  not null,
    publish_time     timestamp,
    consume_time     timestamp,
    retry_count      int          not null default 0,
    max_retry_count  int          not null default 3,
    next_retry_time  timestamp,
    result_msg       varchar(255),
    created_at       timestamp    not null default current_timestamp,
    updated_at       timestamp    not null default current_timestamp
);

create index if not exists idx_callback_task_next_retry_time
    on callback_task (status, next_retry_time);

-- 2. Outbox（待发消息表）
create table if not exists callback_outbox (
    id                  uuid primary key,
    task_id             uuid         not null,
    topic_name          varchar(128) not null,
    message_key         varchar(128) not null,
    message_payload     jsonb        not null,
    status              varchar(32)  not null,  -- NEW | PUBLISHED | PUBLISH_FAILED
    publish_retry_count int          not null default 0,
    next_attempt_time   timestamp    not null,
    last_error_msg      varchar(255),
    created_at          timestamp    not null default current_timestamp,
    updated_at          timestamp    not null default current_timestamp,
    constraint fk_callback_outbox_task
        foreign key (task_id) references callback_task (id)
);

create index if not exists idx_callback_outbox_status_next_attempt
    on callback_outbox (status, next_attempt_time);

create index if not exists idx_callback_outbox_task_id
    on callback_outbox (task_id);

-- 3. 消费日志
create table if not exists callback_consume_log (
    id              uuid primary key,
    task_id         uuid         not null,
    consumer_group  varchar(128) not null,
    topic_name      varchar(128) not null,
    partition_no    int          not null,
    offset_no       bigint       not null,
    simulate_mode   varchar(16)  not null,
    consume_result  varchar(32)  not null,  -- SUCCESS | FAILED | IGNORED
    error_msg       varchar(255),
    created_at      timestamp    not null default current_timestamp,
    constraint fk_callback_consume_log_task
        foreign key (task_id) references callback_task (id)
);
```

增量升级脚本见 `src/main/resources/sql/outbox_upgrade.sql`（为已有 `callback_task` 表追加重试字段与 `callback_outbox` 表）。

## Entity 对照

| 表名 | Entity 类 |
|------|-----------|
| `callback_task` | `CallbackTaskEntity` |
| `callback_outbox` | `CallbackOutboxEntity` |
| `callback_consume_log` | `CallbackConsumeLogEntity` |

## 学到了什么

- Outbox 如何将「写库」与「发消息」在逻辑上绑定。
- Relay 定时补偿 vs 同步发 Kafka 的可靠性差异。
- 消费端重试、任务状态机与 `callback_consume_log` 审计。
