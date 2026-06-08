package com.kafkalearn.api.dto;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * 流水明细响应 DTO。
 *
 * @param txId         流水 ID
 * @param accountId    账户 ID
 * @param eventId      来源事件 ID
 * @param eventType    事件类型
 * @param amount       变动金额
 * @param balanceAfter 变动后余额
 * @param occurredAt   发生时间
 */
public record TransactionResponse(
        UUID txId,
        UUID accountId,
        UUID eventId,
        String eventType,
        BigDecimal amount,
        BigDecimal balanceAfter,
        Instant occurredAt
) {
}
