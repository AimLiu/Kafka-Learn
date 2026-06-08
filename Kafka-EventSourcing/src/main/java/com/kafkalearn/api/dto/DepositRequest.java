package com.kafkalearn.api.dto;

import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotNull;

import java.math.BigDecimal;
import java.util.UUID;

/**
 * 充值请求 DTO。
 *
 * @param commandId        命令幂等 ID
 * @param amount           充值金额
 * @param expectedVersion  期望版本（乐观锁，可选）
 */
public record DepositRequest(
        @NotNull(message = "commandId 不能为空")
        UUID commandId,
        @NotNull(message = "amount 不能为空")
        @DecimalMin(value = "0.01", message = "amount 必须大于 0")
        BigDecimal amount,
        Long expectedVersion
) {
}
