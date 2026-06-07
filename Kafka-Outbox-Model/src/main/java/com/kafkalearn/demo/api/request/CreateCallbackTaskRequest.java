package com.kafkalearn.demo.api.request;

import com.fasterxml.jackson.databind.JsonNode;
import com.kafkalearn.demo.domain.SimulateMode;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

public record CreateCallbackTaskRequest(
        @NotBlank(message = "bizNo 不能为空")
        String bizNo,
        @NotNull(message = "payload 不能为空")
        JsonNode payload,
        @NotNull(message = "simulateMode 不能为空")
        SimulateMode simulateMode
) {
}
