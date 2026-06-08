# Kafka-EventSourcing

事件溯源（Event Sourcing）学习模块，场景为 **银行账户**：开户、充值、扣款、关户；以 PostgreSQL 事件表为真相源，Kafka 驱动读侧投影，并包含 **快照** 与 **Replay 对账 API**。

> 详细场景、架构与 API 说明见 [docs/EventSourcing学习版-设计文档.md](../docs/EventSourcing学习版-设计文档.md)

## 架构（方案 A）

```
Command API → AccountAggregate → es_event（PG Event Store）
                              → Kafka account-events
                              → AccountProjector → 读模型表
Snapshot / Replay API → 直接读 es_event（仍在方案 A 内，不替代 PG + Kafka）
```

**快照** 与 **Replay** 是方案 A 的性能/验证增强，不是另一种架构。

## 涉及场景一览

| 编号 | 场景 | 说明 |
|------|------|------|
| S1 | 开户 | `AccountOpened` |
| S2 | 充值 | `MoneyDeposited` |
| S3 | 扣款 | `MoneyWithdrawn`，余额不足拒绝 |
| S4 | 查询余额 | 读 `es_account_balance_view` |
| S5 | 查询流水 | 读 `es_account_transaction_view` |
| S6 | 关闭账户 | `AccountClosed`（可选） |
| S7 | 并发冲突 | `current_version` 乐观锁 |
| S8 | 命令幂等 | `es_command_dedup` |
| S9 | 异步投影 | Kafka → Projector |
| S10 | 投影幂等 | 按 `event_id` 去重 |
| S11 | 写快照 | `es_account_snapshot` |
| S12 | Replay 对账 | 重放事件 vs 读模型 |
| S13 | Kafka 故障 | 写侧以 PG 为准（见设计文档） |
| S14 | 读模型重建 | 从 `es_event` 全量重投影 |

## 运行（模块实现后）

```bash
# 在项目根目录
mvn clean install

# 启动本模块
cd Kafka-EventSourcing
mvn spring-boot:run
```

主类（待实现）：`com.kafkalearn.KafkaEventSourcingApplication`

## 关键配置

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `APP_KAFKA_BOOTSTRAP_SERVERS` | `192.168.19.64:9092` | Kafka 地址 |
| `APP_KAFKA_CONSUMER_GROUP` | `account-projection-group` | 投影消费者组 |
| `APP_DB_HOST` / `APP_DB_NAME` | `192.168.19.64` / `kafka-learn` | PostgreSQL |
| `app.kafka.account-events-topic` | `account-events` | 账户事件 Topic |

## 数据库建表

启动前在 PostgreSQL 执行以下 SQL（建议 `ddl-auto: none`，手工建表）：

```sql
-- 1. 聚合根元数据
create table if not exists es_account
(
    account_id      uuid primary key,
    owner_name      varchar(128) not null,
    status          varchar(32)  not null default 'ACTIVE',
    current_version bigint       not null default 0,
    created_at      timestamp    not null default current_timestamp,
    updated_at      timestamp    not null default current_timestamp,
    constraint chk_es_account_status check (status in ('ACTIVE', 'CLOSED'))
);

-- 2. Event Store（append-only）
create table if not exists es_event
(
    event_id        uuid         primary key,
    aggregate_type  varchar(64)  not null default 'Account',
    aggregate_id    uuid         not null,
    event_type      varchar(64)  not null,
    event_version   int          not null default 1,
    sequence        bigint       not null,
    command_id      uuid,
    payload         jsonb        not null,
    metadata        jsonb,
    occurred_at     timestamp    not null default current_timestamp,
    constraint uq_es_event_aggregate_sequence unique (aggregate_id, sequence),
    constraint fk_es_event_account foreign key (aggregate_id) references es_account (account_id)
);

create index if not exists idx_es_event_aggregate_seq on es_event (aggregate_id, sequence);
create index if not exists idx_es_event_occurred_at on es_event (occurred_at);

-- 3. 快照
create table if not exists es_account_snapshot
(
    account_id       uuid primary key,
    snapshot_version bigint         not null,
    balance          numeric(18, 2) not null,
    snapshot_payload jsonb,
    created_at       timestamp      not null default current_timestamp,
    constraint fk_es_snapshot_account foreign key (account_id) references es_account (account_id)
);

-- 4. 余额读模型
create table if not exists es_account_balance_view
(
    account_id          uuid primary key,
    owner_name          varchar(128)   not null,
    balance             numeric(18, 2) not null default 0,
    status              varchar(32)    not null default 'ACTIVE',
    last_event_sequence bigint         not null default 0,
    updated_at          timestamp      not null default current_timestamp,
    constraint fk_es_balance_view_account foreign key (account_id) references es_account (account_id),
    constraint chk_es_balance_view_status check (status in ('ACTIVE', 'CLOSED'))
);

-- 5. 流水读模型
create table if not exists es_account_transaction_view
(
    tx_id         uuid primary key,
    account_id    uuid           not null,
    event_id      uuid           not null,
    event_type    varchar(64)    not null,
    amount        numeric(18, 2) not null,
    balance_after numeric(18, 2) not null,
    occurred_at   timestamp      not null,
    constraint uq_es_tx_event_id unique (event_id),
    constraint fk_es_tx_account foreign key (account_id) references es_account (account_id),
    constraint fk_es_tx_event foreign key (event_id) references es_event (event_id)
);

create index if not exists idx_es_tx_account_time on es_account_transaction_view (account_id, occurred_at desc);

-- 6. 命令幂等
create table if not exists es_command_dedup
(
    command_id      uuid primary key,
    command_type    varchar(64) not null,
    aggregate_id    uuid,
    status          varchar(32) not null default 'PROCESSING',
    result_event_id uuid,
    error_message   varchar(512),
    created_at      timestamp   not null default current_timestamp,
    updated_at      timestamp   not null default current_timestamp,
    constraint chk_es_command_status check (status in ('PROCESSING', 'SUCCEEDED', 'FAILED'))
);

-- 7. 投影检查点（可选）
create table if not exists es_projection_checkpoint
(
    projection_name varchar(128) primary key,
    last_event_id   uuid,
    last_global_seq bigint,
    updated_at      timestamp not null default current_timestamp
);
```

## API 速查

| 类型 | 方法 | 路径 |
|------|------|------|
| 写 | POST | `/api/accounts` |
| 写 | POST | `/api/accounts/{id}/deposit` |
| 写 | POST | `/api/accounts/{id}/withdraw` |
| 写 | POST | `/api/accounts/{id}/close` |
| 读 | GET | `/api/accounts/{id}/balance` |
| 读 | GET | `/api/accounts/{id}/transactions` |

### 示例

```bash
# 开户
curl -X POST http://localhost:8080/api/accounts \
  -H "Content-Type: application/json" \
  -d '{"commandId":"11111111-1111-1111-1111-111111111111","ownerName":"张三","initialBalance":100.00}'

# 充值
curl -X POST http://localhost:8080/api/accounts/{accountId}/deposit \
  -H "Content-Type: application/json" \
  -d '{"commandId":"22222222-2222-2222-2222-222222222222","amount":50.00}'

# 查询余额（投影异步，可能需要短暂等待）
curl http://localhost:8080/api/accounts/{accountId}/balance
```

## 说明

- 写侧：`AccountCommandService` → `EventStoreDao` 追加 `es_event` → 事务提交后发 Kafka。
- 读侧：`AccountEventProjector` 消费 `account-events` → 更新读模型表。
- Kafka 配置复用 `KafkaAutoConfiguration` + `KafkaProperties`（与 EventDriven 模块同模式）。
- 事件编解码使用 `AccountEventCodec`（JSON 字符串 + `StringSerializer`）。

## 与 Kafka-EventDriven 的区别

| | EventDriven | EventSourcing |
|---|---|---|
| 真相源 | 当前状态表 | `es_event` 事件流 |
| 历史 | 无 | 完整保留 |
| 余额/状态 | 直接 UPDATE | 事件重放或投影 |
| Kafka 角色 | 主链路传递 | 写库后分发、驱动投影 |
