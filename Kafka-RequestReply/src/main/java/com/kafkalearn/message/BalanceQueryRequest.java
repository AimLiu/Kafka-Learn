package com.kafkalearn.message;

import java.util.UUID;

/**
 * @author: Mafeifei
 * @email: owntnow@163.com
 * @Date: 2026/6/12 22:47
 * @Description:
 */
public record BalanceQueryRequest(UUID accountId, long requestedAt) {
}