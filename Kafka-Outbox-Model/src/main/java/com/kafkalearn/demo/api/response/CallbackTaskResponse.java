package com.kafkalearn.demo.api.response;

import com.fasterxml.jackson.databind.JsonNode;
import com.kafkalearn.demo.domain.SimulateMode;
import com.kafkalearn.demo.domain.TaskStatus;

import java.time.LocalDateTime;
import java.util.UUID;

public record CallbackTaskResponse(
        UUID id,
        String bizNo,
        String topicName,
        JsonNode payload,
        SimulateMode simulateMode,
        TaskStatus status,
        String traceId,
        LocalDateTime publishTime,
        LocalDateTime consumeTime,
        int retryCount,
        int maxRetryCount,
        LocalDateTime nextRetryTime,
        String resultMsg,
        LocalDateTime createdAt,
        LocalDateTime updatedAt
) {
}
