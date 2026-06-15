package com.kafkalearn.messaging;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * JSON 与 Java 消息对象之间的编解码器。
 *
 * <p>Kafka 线上传输 JSON 字符串；Streams / KafkaTemplate 边界处调用本类。
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class EventJsonCodec {

    private final ObjectMapper objectMapper;

    /**
     * 暴露 ObjectMapper，供 JsonSerde 与 Topology 共用同一 Jackson 配置。
     *
     * @return ObjectMapper 实例
     */
    public ObjectMapper getObjectMapper() {
        return objectMapper;
    }

    /**
     * 将对象序列化为 JSON 字符串。
     *
     * @param value 消息对象
     * @return JSON 字符串
     */
    public String serialize(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("事件序列化失败: " + value, e);
        }
    }

    /**
     * 将 JSON 字符串反序列化为指定类型。
     *
     * @param json  JSON 字符串
     * @param type  目标类型
     * @param <T>   类型参数
     * @return 反序列化结果；失败时返回 null 并由调用方过滤
     */
    public <T> T deserialize(String json, Class<T> type) {
        try {
            return objectMapper.readValue(json, type);
        } catch (JsonProcessingException e) {
            // 非法 JSON 由 Topology filter 丢弃；仅记录原因，不打印堆栈（学习样例会故意触发）
            log.warn("事件反序列化失败, type={}, reason={}, payload={}",
                    type.getSimpleName(), e.getOriginalMessage(), json);
            return null;
        }
    }
}
