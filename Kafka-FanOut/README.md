# Kafka-FanOut

设备告警 Fan-out + DLQ 学习模块。一条告警事件经 `device-alert-topic` 被三个 Consumer Group 独立消费（存储 / 推送 / 规则引擎），失败消息进入各自 DLQ 并归档。

设计文档：[../docs/Kafka-FanOut-DLQ-设计文档.md](../docs/Kafka-FanOut-DLQ-设计文档.md)

## 运行

```bash
docker compose up -d   # 项目根目录，启动 Kafka
cd Kafka-FanOut
mvn spring-boot:run
```

主类：`com.kafkalearn.KafkaFanOutApplication`

## Simulator 学习场景

`DeviceAlertSimulator` 提供两类自动发送：

| 调度 | 配置项 | 默认 | 说明 |
|------|--------|------|------|
| 随机 SUCCESS | `app.simulator.random-interval-ms` | 5000 | 覆盖 L1，保证三表持续有数据 |
| 学习样例轮转 | `app.simulator.learning-cycle-interval-ms` | 30000 | 覆盖 L2–L10；可用 `learning-cycle-enabled: false` 关闭 |

| 编号 | 场景 | 触发方式 | build 方法 | 期望 |
|------|------|----------|------------|------|
| L1 | Fan-out 正常 | 随机 5s | `buildRandomSuccessAlert()` | 三表均有数据 |
| L2 | Push SMS | 轮转 | `buildCriticalPushAlert()` | push_log.channel=SMS |
| L3 | Push APP | 轮转 | `buildAppPushAlert()` | push_log.channel=APP |
| L4 | 规则 R1 | 轮转 | `buildCriticalRuleAlert()` | CRITICAL_ESCALATE / ESCALATE |
| L5 | 规则 R2 | 轮转 | `buildTempThresholdAlert()` | TEMP_THRESHOLD / OPEN_TICKET |
| L6 | 规则未命中 | 轮转 | `buildNoRuleHitAlert()` | rule 表无记录 |
| L7 | Storage DLQ | 轮转 | `buildFailStorageAlert()` | storage DLQ + archive |
| L8 | Push DLQ | 轮转 | `buildFailPushAlert()` | push DLQ + archive |
| L9 | Rule DLQ | 轮转 | `buildFailRuleAlert()` | rule DLQ + archive |
| L10 | 多 alertType | 轮转 | `buildSmokeAlert()` / `buildOfflineAlert()` | storage 有记录 |

### 验证 SQL（L8 FAIL_PUSH 示例）

从日志中找到 `eventId` 后：

```sql
SELECT * FROM alert_push_log WHERE event_id = '<eventId>';
SELECT * FROM alert_dlq_archive
WHERE target_consumer LIKE '%Push%'
ORDER BY archived_at DESC LIMIT 5;
SELECT count(*) FROM alert_storage_log;
```

期望：push 表无该 eventId；archive 有 Push 相关记录；storage 仍有数据。

### 幂等实验（§10#5，HTTP 手动）

```bash
curl -X POST http://localhost:8080/alerts/trigger \
  -H "Content-Type: application/json" \
  -d '{"eventId":"11111111-1111-1111-1111-111111111111","occurredAt":1710000000000,
       "deviceId":"c542567a-dfb1-4af7-940a-a5cada4372b6","alertType":"TEMP_HIGH",
       "severity":"WARN","metricValue":75.0,"source":"manual","simulateMode":"SUCCESS"}'
```

同一 body POST 两次，确认三表 `event_id` 不重复 insert。

### DLQ 手动触发（可选）

```bash
curl -X POST http://localhost:8080/alerts/trigger \
  -H "Content-Type: application/json" \
  -d '{"eventId":"22222222-2222-2222-2222-222222222222","occurredAt":1710000000000,
       "deviceId":"c542567a-dfb1-4af7-940a-a5cada4372b6","alertType":"TEMP_HIGH",
       "severity":"CRITICAL","metricValue":90.0,"source":"manual","simulateMode":"FAIL_PUSH"}'
```

## 配置

| 环境变量 | 默认值 | 说明 |
|----------|--------|------|
| `APP_KAFKA_BOOTSTRAP_SERVERS` | `192.168.19.64:9092` | Kafka |
| `APP_DB_HOST` | `192.168.19.64` | PostgreSQL |
| `APP_SERVER_PORT` | `8080` | HTTP 端口 |

关闭学习轮转（避免 DLQ 样例刷屏）：

```yaml
app:
  simulator:
    learning-cycle-enabled: false
```

## 单测

```bash
mvn -pl Kafka-FanOut test -Dtest=DeviceAlertSimulatorTest
```
