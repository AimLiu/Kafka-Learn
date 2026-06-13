package com.kafkalearn.message;

import java.math.BigDecimal;
import java.util.UUID;

/**
 * @author: Mafeifei
 * @email: owntnow@163.com
 * @Date: 2026/6/12 22:47
 * @Description:
 */

public record BalanceQueryResponse(
        UUID accountId,
        BigDecimal balance,
        ReplyStatus status,
        String message,
        long respondedAt
) {
}