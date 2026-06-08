package com.kafkalearn.api.dto;

import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.math.BigDecimal;
import java.util.UUID;

/**
 * 开户请求 DTO。
 *
 * @param commandId       命令幂等 ID
 * @param ownerName       户名
 * @param initialBalance  初始余额
 */
public record OpenAccountRequest(
        @NotNull(message = "commandId 不能为空")
        UUID commandId,
        @NotBlank(message = "ownerName 不能为空")
        String ownerName,
        @NotNull(message = "initialBalance 不能为空")
        @DecimalMin(value = "0.00", message = "initialBalance 不能为负数")
        BigDecimal initialBalance
) {
}
