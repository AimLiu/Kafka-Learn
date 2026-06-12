# Kafka-EventSourcing

事件溯源（Event Sourcing）学习模块：银行账户场景（开户、充值、扣款、关户），以 PostgreSQL `es_event` 为真相源，Kafka 驱动读侧异步投影。

## 设计思想

**状态变更不直接 UPDATE，而是追加不可变事件**；当前余额等读模型由事件投影得出。

```
Command API → AccountCommandService → AccountAggregate
                                   → es_event（PG Event Store，append-only）
                                   → Kafka account-events
                                   → AccountEventProjector → 读模型表
```

方案 A 要点：

- **写侧真相源**：`es_event` 事件表（非 Kafka）。
- **Kafka 角色**：事务提交后分发事件，驱动投影 Consumer。
- **读模型**：`es_account_balance_view`、`es_account_transaction_view` 异步更新。
- **命令幂等**：`es_command_dedup`；**并发控制**：`es_account.current_version` 乐观锁。

## 用到的 Kafka 特性

| 特性 | 本模块用法 |
|------|------------|
| Producer | 写库成功后发 `account-events` Topic |
| Consumer | `AccountEventProjector` 投影消费者组 `account-projection-group` |
| 序列化 | JSON 字符串 + `StringSerializer`（`AccountEventCodec`） |
| 与 DB 关系 | PG 为准；Kafka 故障时写侧仍可用（事件已在 PG） |
| 投影幂等 | 按 `event_id` 去重写入读模型 |

**未在本模块 Entity 中实现**：`es_account_snapshot`、`es_projection_checkpoint`（README 保留 SQL 供扩展，尚无对应 JPA 实体）。

## 涉及场景一览

| 编号 | 场景 | 说明 |
|------|------|------|
| S1 | 开户 | `AccountOpened` |
| S2 | 充值 | `MoneyDeposited` |
| S3 | 扣款 | `MoneyWithdrawn`，余额不足拒绝 |
| S4 | 查询余额 | 读 `es_account_balance_view` |
| S5 | 查询流水 | 读 `es_account_transaction_view` |
| S6 | 关闭账户 | `AccountClosed` |
| S7 | 并发冲突 | `current_version` 乐观锁 |
| S8 | 命令幂等 | `es_command_dedup` |
| S9 | 异步投影 | Kafka → Projector |
| S10 | 投影幂等 | 按 `event_id` 去重 |

## 运行

```bash
docker compose up -d

mvn clean install
cd Kafka-EventSourcing
mvn spring-boot:run
```

主类：`com.kafkalearn.KafkaEventSourcingApplication`

## 关键配置

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `APP_KAFKA_BOOTSTRAP_SERVERS` | `192.168.19.64:9092` | Kafka 地址 |
| `APP_KAFKA_CONSUMER_GROUP` | `account-projection-group` | 投影消费者组 |
| `APP_DB_HOST` / `APP_DB_NAME` | `192.168.19.64` / `kafka-learn` | PostgreSQL |
| `app.kafka.account-events-topic` | `account-events` | 账户事件 Topic |

建议 `ddl-auto: none`，手工建表。

## 数据库建表

以下 SQL 与现有 Entity 对齐。时间字段 Entity 使用 `Instant`，对应 PostgreSQL `timestamp`。

```sql
-- 1. 聚合根元数据 → EsAccount
create table if not exists es_account (
    account_id      uuid primary key,
    owner_name      varchar(128) not null,
    status          varchar(32)  not null default 'ACTIVE',  -- ACTIVE | CLOSED
    current_version bigint       not null default 0,
    created_at      timestamp    not null default current_timestamp,
    updated_at      timestamp    not null default current_timestamp,
    constraint chk_es_account_status check (status in ('ACTIVE', 'CLOSED'))
);

-- 2. Event Store（append-only）→ EsEvent
create table if not exists es_event (
    event_id        uuid primary key,
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

-- 3. 余额读模型 → EsAccountBalanceView
create table if not exists es_account_balance_view (
    account_id          uuid primary key,
    owner_name          varchar(128)   not null,
    balance             numeric(18, 2) not null default 0,
    status              varchar(32)    not null default 'ACTIVE',
    last_event_sequence bigint         not null default 0,
    updated_at          timestamp      not null default current_timestamp,
    constraint fk_es_balance_view_account foreign key (account_id) references es_account (account_id),
    constraint chk_es_balance_view_status check (status in ('ACTIVE', 'CLOSED'))
);

-- 4. 流水读模型 → EsAccountTransactionView
create table if not exists es_account_transaction_view (
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

create index if not exists idx_es_tx_account_time
    on es_account_transaction_view (account_id, occurred_at desc);

-- 5. 命令幂等 → EsCommandDedup
create table if not exists es_command_dedup (
    command_id      uuid primary key,
    command_type    varchar(64) not null,
    aggregate_id    uuid,
    status          varchar(32) not null default 'PROCESSING',  -- PROCESSING | SUCCEEDED | FAILED
    result_event_id uuid,
    error_message   varchar(512),
    created_at      timestamp   not null default current_timestamp,
    updated_at      timestamp   not null default current_timestamp,
    constraint chk_es_command_status check (status in ('PROCESSING', 'SUCCEEDED', 'FAILED'))
);
```

### 扩展表（设计预留，当前无 Entity）

```sql
-- 快照（S11，待实现 EsAccountSnapshot 实体）
create table if not exists es_account_snapshot (
    account_id       uuid primary key,
    snapshot_version bigint         not null,
    balance          numeric(18, 2) not null,
    snapshot_payload jsonb,
    created_at       timestamp      not null default current_timestamp,
    constraint fk_es_snapshot_account foreign key (account_id) references es_account (account_id)
);

-- 投影检查点（可选，待实现实体）
create table if not exists es_projection_checkpoint (
    projection_name varchar(128) primary key,
    last_event_id   uuid,
    last_global_seq bigint,
    updated_at      timestamp not null default current_timestamp
);
```

## Entity 对照

| 表名 | Entity 类 |
|------|-----------|
| `es_account` | `EsAccount` |
| `es_event` | `EsEvent` |
| `es_account_balance_view` | `EsAccountBalanceView` |
| `es_account_transaction_view` | `EsAccountTransactionView` |
| `es_command_dedup` | `EsCommandDedup` |

## API 速查

| 类型 | 方法 | 路径 |
|------|------|------|
| 写 | POST | `/api/accounts` |
| 写 | POST | `/api/accounts/{id}/deposit` |
| 写 | POST | `/api/accounts/{id}/withdraw` |
| 写 | POST | `/api/accounts/{id}/close` |
| 读 | GET | `/api/accounts/{id}/balance` |
| 读 | GET | `/api/accounts/{id}/transactions` |

```bash
curl -X POST http://localhost:8080/api/accounts \
  -H "Content-Type: application/json" \
  -d '{"commandId":"11111111-1111-1111-1111-111111111111","ownerName":"张三","initialBalance":100.00}'
```

## 与 Kafka-EventDriven 的区别

| | EventDriven | EventSourcing |
|---|---|---|
| 真相源 | 当前状态表 | `es_event` 事件流 |
| 历史 | 无 | 完整保留 |
| 状态更新 | 直接 UPDATE | 事件追加 + 投影 |
| Kafka 角色 | 主链路传递 | 写库后分发、驱动投影 |

## 学到了什么

- Event Sourcing 下「写」与「读」分离（CQRS 雏形）。
- Kafka 是投影触发器，不是事件唯一存储。
- 命令幂等与乐观锁在分布式写侧的必要性。
