package com.kafkalearn.messaging;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafkalearn.exception.EventPublishException;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

/**
 * 账户领域事件 JSON 编解码器。
 */
@Component
@RequiredArgsConstructor
public class AccountEventCodec {

    private final ObjectMapper objectMapper;

    /**
     * 将事件消息序列化为 JSON 字符串。
     *
     * @param message 事件消息
     * @return JSON 字符串
     */
    public String serialize(AccountDomainEventMessage message) {
        try {
            return objectMapper.writeValueAsString(message);
        } catch (JsonProcessingException e) {
            throw new EventPublishException("账户事件序列化失败", e);
        }
    }

    /**
     * 将 JSON 字符串反序列化为事件消息。
     *
     * @param payload JSON 字符串
     * @return 事件消息
     */
    public AccountDomainEventMessage deserialize(String payload) {
        try {
            return objectMapper.readValue(payload, AccountDomainEventMessage.class);
        } catch (JsonProcessingException e) {
            throw new EventPublishException("账户事件反序列化失败", e);
        }
    }
}
