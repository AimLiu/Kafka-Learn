package com.kafkalearn.api.dto;

import jakarta.validation.constraints.NotNull;

import java.util.UUID;

/**
 * 关户请求 DTO。
 *
 * @param commandId        命令幂等 ID
 * @param reason           关户原因
 * @param expectedVersion  期望版本（乐观锁，可选）
 */
public record CloseAccountRequest(
        @NotNull(message = "commandId 不能为空")
        UUID commandId,
        String reason,
        Long expectedVersion
) {
}
