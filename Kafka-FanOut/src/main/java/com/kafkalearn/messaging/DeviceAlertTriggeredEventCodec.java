package com.kafkalearn.messaging;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafkalearn.event.DeviceAlertTriggeredEvent;
import com.kafkalearn.exception.EventPublishException;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class DeviceAlertTriggeredEventCodec {

    private final ObjectMapper objectMapper;

    public String serialize(DeviceAlertTriggeredEvent event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            throw new EventPublishException("事件序列化失败", e);
        }
    }

    public DeviceAlertTriggeredEvent deserialize(String payload) {
        try {
            return objectMapper.readValue(payload, DeviceAlertTriggeredEvent.class);
        } catch (JsonProcessingException e) {
            throw new EventPublishException("事件反序列化失败", e);
        }
    }
}
