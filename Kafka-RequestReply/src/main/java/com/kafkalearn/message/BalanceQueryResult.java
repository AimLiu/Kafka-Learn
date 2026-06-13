package com.kafkalearn.message;

/**
 * @author: Mafeifei
 * @email: owntnow@163.com
 * @Date: 2026/6/13 11:55
 * @Description:
 */
public record BalanceQueryResult(
        String correlationId,
        BalanceQueryResponse response,
        long elapsedMs
) {
}
