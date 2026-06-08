package com.kafkalearn.domain;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.kafkalearn.domain.event.AccountEventType;
import com.kafkalearn.entity.EsAccount;
import com.kafkalearn.entity.EsEvent;
import com.kafkalearn.exception.AccountClosedException;
import com.kafkalearn.exception.InsufficientBalanceException;
import lombok.Getter;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * 账户聚合根：通过重放事件得到当前状态，并产生新的领域事件。
 */
@Getter
public class AccountAggregate {

    private static final String AGGREGATE_TYPE = "Account";

    private UUID accountId;
    private String ownerName;
    private BigDecimal balance = BigDecimal.ZERO;
    private AccountStatus status = AccountStatus.ACTIVE;
    private long version;

    /**
     * 从元数据与事件流重建聚合。
     *
     * @param account 账户元数据
     * @param events  按 sequence 排序的事件列表
     * @return 重建后的聚合
     */
    public static AccountAggregate replay(EsAccount account, List<EsEvent> events) {
        AccountAggregate aggregate = new AccountAggregate();
        aggregate.accountId = account.getAccountId();
        aggregate.ownerName = account.getOwnerName();
        aggregate.status = account.getStatus();
        aggregate.version = account.getCurrentVersion();
        for (EsEvent event : events) {
            aggregate.apply(event);
        }
        return aggregate;
    }

    /**
     * 构建开户事件（新聚合）。
     *
     * @param accountId      账户 ID
     * @param ownerName      户名
     * @param initialBalance 初始余额
     * @param commandId      命令 ID
     * @param objectMapper   JSON 工具
     * @return 待持久化事件
     */
    public static PendingEvent open(UUID accountId,
                                    String ownerName,
                                    BigDecimal initialBalance,
                                    UUID commandId,
                                    ObjectMapper objectMapper) {
        if (initialBalance.compareTo(BigDecimal.ZERO) < 0) {
            throw new IllegalArgumentException("初始余额不能为负数");
        }
        ObjectNode payload = objectMapper.createObjectNode();
        payload.put("ownerName", ownerName);
        payload.put("initialBalance", initialBalance);
        payload.put("openedAt", Instant.now().toEpochMilli());
        return new PendingEvent(
                UUID.randomUUID(),
                accountId,
                AGGREGATE_TYPE,
                AccountEventType.ACCOUNT_OPENED,
                1L,
                commandId,
                payload,
                Instant.now()
        );
    }

    /**
     * 校验并产生充值事件。
     *
     * @param amount       充值金额
     * @param commandId    命令 ID
     * @param objectMapper JSON 工具
     * @return 待持久化事件
     */
    public PendingEvent deposit(BigDecimal amount, UUID commandId, ObjectMapper objectMapper) {
        ensureActive();
        if (amount.compareTo(BigDecimal.ZERO) <= 0) {
            throw new IllegalArgumentException("充值金额必须大于 0");
        }
        ObjectNode payload = objectMapper.createObjectNode();
        payload.put("amount", amount);
        payload.put("commandId", commandId.toString());
        payload.put("depositedAt", Instant.now().toEpochMilli());
        return nextEvent(AccountEventType.MONEY_DEPOSITED, commandId, payload);
    }

    /**
     * 校验并产生扣款事件。
     *
     * @param amount       扣款金额
     * @param commandId    命令 ID
     * @param objectMapper JSON 工具
     * @return 待持久化事件
     */
    public PendingEvent withdraw(BigDecimal amount, UUID commandId, ObjectMapper objectMapper) {
        ensureActive();
        if (amount.compareTo(BigDecimal.ZERO) <= 0) {
            throw new IllegalArgumentException("扣款金额必须大于 0");
        }
        if (balance.compareTo(amount) < 0) {
            throw new InsufficientBalanceException("余额不足，当前余额=" + balance);
        }
        ObjectNode payload = objectMapper.createObjectNode();
        payload.put("amount", amount);
        payload.put("commandId", commandId.toString());
        payload.put("withdrawnAt", Instant.now().toEpochMilli());
        return nextEvent(AccountEventType.MONEY_WITHDRAWN, commandId, payload);
    }

    /**
     * 校验并产生关户事件。
     *
     * @param commandId    命令 ID
     * @param reason       关户原因
     * @param objectMapper JSON 工具
     * @return 待持久化事件
     */
    public PendingEvent close(UUID commandId, String reason, ObjectMapper objectMapper) {
        ensureActive();
        if (balance.compareTo(BigDecimal.ZERO) != 0) {
            throw new IllegalArgumentException("关户前余额必须为 0，当前余额=" + balance);
        }
        ObjectNode payload = objectMapper.createObjectNode();
        payload.put("closedAt", Instant.now().toEpochMilli());
        payload.put("reason", reason);
        return nextEvent(AccountEventType.ACCOUNT_CLOSED, commandId, payload);
    }

    private PendingEvent nextEvent(String eventType, UUID commandId, JsonNode payload) {
        return new PendingEvent(
                UUID.randomUUID(),
                accountId,
                AGGREGATE_TYPE,
                eventType,
                version + 1,
                commandId,
                payload,
                Instant.now()
        );
    }

    private void apply(EsEvent event) {
        switch (event.getEventType()) {
            case AccountEventType.ACCOUNT_OPENED -> applyOpened(event.getPayload());
            case AccountEventType.MONEY_DEPOSITED -> applyDeposited(event.getPayload());
            case AccountEventType.MONEY_WITHDRAWN -> applyWithdrawn(event.getPayload());
            case AccountEventType.ACCOUNT_CLOSED -> status = AccountStatus.CLOSED;
            default -> throw new IllegalStateException("未知事件类型: " + event.getEventType());
        }
        version = event.getSequence();
    }

    private void applyOpened(JsonNode payload) {
        ownerName = payload.get("ownerName").asText();
        balance = payload.get("initialBalance").decimalValue();
        status = AccountStatus.ACTIVE;
    }

    private void applyDeposited(JsonNode payload) {
        balance = balance.add(payload.get("amount").decimalValue());
    }

    private void applyWithdrawn(JsonNode payload) {
        balance = balance.subtract(payload.get("amount").decimalValue());
    }

    private void ensureActive() {
        if (status == AccountStatus.CLOSED) {
            throw new AccountClosedException("账户已关闭，无法继续操作");
        }
    }

    /**
     * 待持久化的领域事件描述。
     *
     * @param eventId       事件 ID
     * @param aggregateId   聚合 ID
     * @param aggregateType 聚合类型
     * @param eventType     事件类型
     * @param sequence      序号
     * @param commandId     命令 ID
     * @param payload       事件体
     * @param occurredAt    发生时间
     */
    public record PendingEvent(
            UUID eventId,
            UUID aggregateId,
            String aggregateType,
            String eventType,
            long sequence,
            UUID commandId,
            JsonNode payload,
            Instant occurredAt
    ) {
    }
}
