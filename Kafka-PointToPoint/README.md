# Kafka-PointToPoint

点对点（Queue-like）模式学习模块：同一 Consumer Group 内，每条消息只被一个消费者实例处理，演示 Kafka 的任务分发与竞争消费。

## 设计思想

本模块聚焦 Kafka 最基础的「生产 → 消费」链路，刻意保持极简：

- **一个 Topic、一个 Consumer Group、两个监听器**：同组内多 `@KafkaListener` 实例竞争分区消息，理解「点对点」与「发布订阅」的区别（后者需不同 Group）。
- **无数据库、无 HTTP**：降低干扰，只观察 Producer 发送与 Consumer 日志。
- **定时调度驱动**：`ProduceSheduler` 每 5 秒发一条消息，便于本地观察。

```
ProduceSheduler（定时）
    → KafkaMsgProducer 发送 KafkaSendMsg
    → Topic: kafka-learn-producer
    → MyKafkaConsumer（同 group 内 2 个监听器竞争消费）
```

## 用到的 Kafka 特性

| 特性 | 本模块用法 |
|------|------------|
| Producer API | `KafkaTemplate` + `CompletableFuture` 异步发送 |
| Topic | 单 Topic `kafka-learn-producer` |
| Consumer Group | `producer-consumer-group-1`，两监听器共享 |
| 竞争消费 | 同 Group 内消息只被一个监听器处理 |
| 序列化 | Key: `StringSerializer`；Value: `JsonSerializer`（`KafkaSendMsg`） |
| 压缩 | Producer `compression-type: gzip`（可配置） |
| 调度发送 | `@Scheduled` 模拟业务触发 |

**未涉及**：事务、幂等 Producer、手动 ack、DLQ、分区键策略、数据库。

## 运行

```bash
# 项目根目录启动 Kafka
docker compose up -d

# 构建并运行
cd Kafka-PointToPoint
mvn spring-boot:run
```

主类：`com.kafkalearn.KafkaProducerApplication`

## 关键配置

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `APP_KAFKA_PRODUCER_BOOTSTRAP_SERVERS` | `192.168.19.64:9092` | Kafka 地址 |
| `APP_KAFKA_PRODUCE_TOPIC` | `kafka-learn-producer` | 生产 Topic |
| `APP_SERVER_PORT` | `8080` | HTTP 端口（本模块未暴露业务 API） |
| `app.scheduler.send-delay-ms` | `5000` | 定时发送间隔 |

## 数据库

本模块**无 Entity、无数据库表**。

## 学到了什么

- Consumer Group 决定消息分配方式，不是 Topic 本身。
- 同 Group 多实例可提高吞吐，但每条消息只被一个实例消费。
- 与 Fan-out（多 Group 各消费一次）形成对比，是后续学习的基础。
