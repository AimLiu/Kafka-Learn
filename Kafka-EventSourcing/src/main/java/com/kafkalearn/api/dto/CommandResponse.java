package com.kafkalearn.api.dto;

import java.util.UUID;

/**
 * 写命令统一响应 DTO。
 *
 * @param accountId   账户 ID
 * @param eventId     产生的事件 ID
 * @param version     事件序号（聚合版本）
 * @param idempotent  是否为幂等重复请求
 */
public record CommandResponse(
        UUID accountId,
        UUID eventId,
        long version,
        boolean idempotent
) {
}
