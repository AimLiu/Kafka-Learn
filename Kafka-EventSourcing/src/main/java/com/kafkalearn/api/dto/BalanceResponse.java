package com.kafkalearn.api.dto;

import com.kafkalearn.domain.AccountStatus;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * 余额查询响应 DTO。
 *
 * @param accountId  账户 ID
 * @param ownerName  户名
 * @param balance    当前余额
 * @param status     账户状态
 * @param version    已投影到的最后事件序号
 * @param updatedAt  读模型更新时间
 */
public record BalanceResponse(
        UUID accountId,
        String ownerName,
        BigDecimal balance,
        AccountStatus status,
        long version,
        Instant updatedAt
) {
}
