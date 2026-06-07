package com.kafkalearn.demo.api.response;

import com.kafkalearn.demo.domain.ConsumeResult;
import com.kafkalearn.demo.domain.SimulateMode;

import java.time.LocalDateTime;
import java.util.UUID;

public record CallbackConsumeLogResponse(
        UUID id,
        UUID taskId,
        String consumerGroup,
        String topicName,
        int partitionNo,
        long offsetNo,
        SimulateMode simulateMode,
        ConsumeResult consumeResult,
        String errorMsg,
        LocalDateTime createdAt
) {
}
