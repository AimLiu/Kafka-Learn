package com.kafkalearn.demo.messaging;

import com.kafkalearn.demo.domain.SimulateMode;

import java.time.LocalDateTime;
import java.util.UUID;

public record CallbackEventMessage(
        UUID taskId,
        String bizNo,
        String traceId,
        SimulateMode simulateMode,
        LocalDateTime createdAt
) {
}
