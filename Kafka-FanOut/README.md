# Kafka-FanOut

设备告警 Fan-out + DLQ 学习模块：一条告警事件发布到 `device-alert-topic`，被三个独立 Consumer Group 并行消费（存储 / 推送 / 规则引擎），失败消息进入各自 DLQ 并归档。

## 设计思想

演示 **Pub/Sub + Fan-out** 与 **死信队列** 的组合：

- **一条消息、多个下游**：各 Consumer Group 独立消费同一 Topic，互不影响。
- **各下游专属逻辑**：存储落库、推送渠道选择、规则命中判断。
- **失败隔离**：`DefaultErrorHandler` + `DeadLetterPublishingRecoverer` 将 poison message 转入 DLQ Topic，再由 `AlertArchiveDLQConsumer` 归档。
- **幂等消费**：各表按 `event_id` 去重，应对 at-least-once 重复投递。

```
DeviceAlertSimulator / HTTP API
    → device-alert-topic
    ├─ alert-storage-group   → alert_storage_log
    ├─ alert-push-group      → alert_push_log
    └─ alert-rule-group      → alert_rule_hit_log
    失败 → 各自 DLQ Topic → alert_dlq_archive
```

## 用到的 Kafka 特性

| 特性 | 本模块用法 |
|------|------------|
| Pub/Sub | 同一 Topic，三个不同 Consumer Group 各消费一次 |
| Fan-out | 存储、推送、规则引擎并行处理 |
| Producer 幂等 | `enable-idempotence: true` |
| 可靠性 | `acks=all`、`retries=3`、`snappy` 压缩 |
| 手动 ack | `enable-auto-commit: false`，`ack-mode: record` |
| DLQ | `DeadLetterPublishingRecoverer` + 独立 DLQ Topic（按原分区） |
| 错误重试 | `DefaultErrorHandler` + `FixedBackOff(2000ms, 3次)` |
| 不可重试异常 | `JsonProcessingException`、`IllegalArgumentException` 直接进 DLQ |
| 序列化 | JSON 字符串 + `StringSerializer`（`DeviceAlertTriggeredEventCodec`） |

**DLQ Topics：**

- `device-alert-storage-dlq`
- `device-alert-push-dlq`
- `device-alert-rule-dlq`

## 运行

```bash
docker compose up -d   # 项目根目录
cd Kafka-FanOut
mvn spring-boot:run
```

主类：`com.kafkalearn.KafkaFanOutApplication`

## 关键配置

| 环境变量 | 默认值 | 说明 |
|----------|--------|------|
| `APP_KAFKA_BOOTSTRAP_SERVERS` | `192.168.19.64:9092` | Kafka |
| `APP_DB_HOST` | `192.168.19.64` | PostgreSQL |
| `APP_SERVER_PORT` | `8081` | HTTP 端口 |

关闭学习轮转（避免 DLQ 样例刷屏）：

```yaml
app:
  simulator:
    learning-cycle-enabled: false
```

## 数据库建表

以下 SQL 与 Entity 类字段对齐。也可使用 JPA `ddl-auto: update` 自动建表。

```sql
-- 1. 存储消费日志 → AlertEventEntity
create table if not exists alert_storage_log (
    id            uuid primary key,
    event_id      uuid         not null,
    device_id     uuid         not null,
    alert_type    varchar(32)  not null,  -- TEMP_HIGH | SMOKE | OFFLINE 等
    severity      varchar(32)  not null,  -- INFO | WARN | CRITICAL
    metric_value  double precision,
    source        varchar(255) not null,
    simulate_mode varchar(32)  not null,  -- SUCCESS | FAIL_STORAGE | FAIL_PUSH | FAIL_RULE 等
    occured_time  timestamp    not null,
    stored_at     timestamp    not null
);

create unique index if not exists uq_alert_storage_event_id
    on alert_storage_log (event_id);

-- 2. 推送日志 → AlertPushLogEntity
create table if not exists alert_push_log (
    id          uuid primary key,
    event_id    uuid         not null,
    device_id   uuid         not null,
    channel     varchar(32)  not null,  -- SMS | APP
    push_status varchar(64)  not null,
    pushed_at   timestamp    not null
);

create unique index if not exists uq_alert_push_event_id
    on alert_push_log (event_id);

-- 3. 规则命中日志 → AlertRuleEngineEntity
create table if not exists alert_rule_hit_log (
    id           uuid primary key,
    event_id     uuid         not null,
    device_id    uuid         not null,
    rule_name    varchar(128) not null,
    action       varchar(128) not null,
    processed_at timestamp    not null
);

create unique index if not exists uq_alert_rule_event_id
    on alert_rule_hit_log (event_id);

-- 4. DLQ 归档 → AlertArchiveDLQEntity
-- 注意：rawPayload 列名与 Entity @Column(name = "rawPayload") 一致（驼峰）
create table if not exists alert_dlq_archive (
    id              uuid primary key,
    event_id        uuid         not null,
    source_topic    varchar(128) not null,
    target_consumer varchar(128) not null,
    "rawPayload"    text         not null,
    error_message   text         not null,
    archived_at     timestamp    not null
);
```

## Entity 对照

| 表名 | Entity 类 |
|------|-----------|
| `alert_storage_log` | `AlertEventEntity` |
| `alert_push_log` | `AlertPushLogEntity` |
| `alert_rule_hit_log` | `AlertRuleEngineEntity` |
| `alert_dlq_archive` | `AlertArchiveDLQEntity` |

## Simulator 学习场景

| 调度 | 配置项 | 默认 | 说明 |
|------|--------|------|------|
| 随机 SUCCESS | `app.simulator.random-interval-ms` | 5000 | L1，三表持续有数据 |
| 学习样例轮转 | `app.simulator.learning-cycle-interval-ms` | 30000 | L2–L10 |

| 编号 | 场景 | build 方法 | 期望 |
|------|------|------------|------|
| L1 | Fan-out 正常 | `buildRandomSuccessAlert()` | 三表均有数据 |
| L2 | Push SMS | `buildCriticalPushAlert()` | `channel=SMS` |
| L3 | Push APP | `buildAppPushAlert()` | `channel=APP` |
| L4 | 规则 R1 | `buildCriticalRuleAlert()` | `CRITICAL_ESCALATE` / `ESCALATE` |
| L5 | 规则 R2 | `buildTempThresholdAlert()` | `TEMP_THRESHOLD` / `OPEN_TICKET` |
| L6 | 规则未命中 | `buildNoRuleHitAlert()` | rule 表无记录 |
| L7 | Storage DLQ | `buildFailStorageAlert()` | storage DLQ + archive |
| L8 | Push DLQ | `buildFailPushAlert()` | push DLQ + archive |
| L9 | Rule DLQ | `buildFailRuleAlert()` | rule DLQ + archive |
| L10 | 多 alertType | `buildSmokeAlert()` / `buildOfflineAlert()` | storage 有记录 |

### 验证 SQL（L8 FAIL_PUSH 示例）

```sql
SELECT * FROM alert_push_log WHERE event_id = '<eventId>';
SELECT * FROM alert_dlq_archive
WHERE target_consumer LIKE '%Push%'
ORDER BY archived_at DESC LIMIT 5;
SELECT count(*) FROM alert_storage_log;
```

### HTTP 手动触发

```bash
# 幂等实验：同一 body POST 两次，event_id 不重复 insert
curl -X POST http://localhost:8081/alerts/trigger \
  -H "Content-Type: application/json" \
  -d '{"eventId":"11111111-1111-1111-1111-111111111111","occurredAt":1710000000000,
       "deviceId":"c542567a-dfb1-4af7-940a-a5cada4372b6","alertType":"TEMP_HIGH",
       "severity":"WARN","metricValue":75.0,"source":"manual","simulateMode":"SUCCESS"}'

# DLQ 手动触发
curl -X POST http://localhost:8081/alerts/trigger \
  -H "Content-Type: application/json" \
  -d '{"eventId":"22222222-2222-2222-2222-222222222222","occurredAt":1710000000000,
       "deviceId":"c542567a-dfb1-4af7-940a-a5cada4372b6","alertType":"TEMP_HIGH",
       "severity":"CRITICAL","metricValue":90.0,"source":"manual","simulateMode":"FAIL_PUSH"}'
```

## 单测

```bash
mvn -pl Kafka-FanOut test -Dtest=DeviceAlertSimulatorTest
```

## 学到了什么

- Fan-out 与 Event-Driven 单向事件的区别：多下游各做各的事。
- Consumer Group 是实现 Fan-out 的关键，不是多 Topic。
- DLQ + 归档是隔离 poison message 的常用模式。
- 手动 ack 与 `DefaultErrorHandler` 如何配合工作。
