package com.kafkalearn.event;

import com.fasterxml.jackson.databind.JsonNode;
import com.kafkalearn.event.common.AlertType;
import com.kafkalearn.event.common.Severity;
import com.kafkalearn.event.common.SimulateMode;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.UUID;

public record DeviceAlertTriggeredEvent (
    // 全局幂等键，各下游去重
    String eventId,
    // epoch millis
    long occurredAt,
    // 设备 ID
    UUID deviceId,
    // TEMP_HIGH / SMOKE / OFFLINE
    AlertType alertType,
    // INFO / WARN / CRITICAL
    Severity severity,
    // 可选，如温度 85.5
    Double metricValue,
    //固定 device-alert-simulator
    String source,
    // （DLQ 实验）
    SimulateMode simulateMode
){
}
