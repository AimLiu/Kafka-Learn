package com.kafkalearn.event;

import java.time.Instant;
import java.util.UUID;

public record DeviceStatusChangedEvent(
        String eventId,           // UUID，幂等键
        UUID deviceId,
        ActiveStatus active,      // ACTIVE / DISACTIVE
        long occurredAt,          // 事件发生时间（epoch millis）
        String source             // 固定 "device-status-simulator"
) {
    public static DeviceStatusChangedEvent online(UUID deviceId) {
        DeviceStatusChangedEvent deviceStatusChangedEvent = new DeviceStatusChangedEvent(UUID.randomUUID().toString(),
                deviceId,
                ActiveStatus.ACTIVE,
                Instant.now().toEpochMilli(),
                "device-status-simulator");
        return deviceStatusChangedEvent;

    }

    public static DeviceStatusChangedEvent offline(UUID deviceId) {
        DeviceStatusChangedEvent deviceStatusChangedEvent = new DeviceStatusChangedEvent(UUID.randomUUID().toString(),
                deviceId,
                ActiveStatus.DISACTIVE,
                Instant.now().toEpochMilli(),
                "device-status-simulator");
        return deviceStatusChangedEvent;
    }
}
