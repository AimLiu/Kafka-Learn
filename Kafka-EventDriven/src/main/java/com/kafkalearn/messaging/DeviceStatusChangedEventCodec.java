package com.kafkalearn.messaging;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafkalearn.event.DeviceStatusChangedEvent;
import com.kafkalearn.exception.EventPublishException;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

@Component
public class DeviceStatusChangedEventCodec {

    private final ObjectMapper objectMapper =  new ObjectMapper();

    public String serialize(DeviceStatusChangedEvent event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            throw new EventPublishException("事件序列化失败", e);
        }
    }

    public DeviceStatusChangedEvent deserialize(String payload) {
        try {
            return objectMapper.readValue(payload, DeviceStatusChangedEvent.class);
        } catch (JsonProcessingException e) {
            throw new EventPublishException("事件反序列化失败", e);
        }
    }
}
