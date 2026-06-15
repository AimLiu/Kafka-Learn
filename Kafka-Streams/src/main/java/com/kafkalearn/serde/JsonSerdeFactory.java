package com.kafkalearn.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.util.Collections;
import java.util.Map;

/**
 * 创建 Kafka Streams 使用的 JSON Serde。
 */
public final class JsonSerdeFactory {

    private JsonSerdeFactory() {
    }

    /**
     * 为指定类型创建 JsonSerde。
     *
     * @param type         目标类型
     * @param objectMapper Jackson 实例
     * @param <T>          类型参数
     * @return 可配置到 Grouped / Materialized / Produced 的 Serde
     */
    public static <T> Serde<T> jsonSerde(Class<T> type, ObjectMapper objectMapper) {
        JsonSerde<T> serde = new JsonSerde<>(type, objectMapper);
        serde.configure(getConfig(), false);
        return serde;
    }

    private static Map<String, ?> getConfig() {
        return Collections.emptyMap();
    }

    /**
     * Spring 容器内注册 ObjectMapper Bean 的配置类引用占位。
     * 实际 ObjectMapper 由 {@link com.kafkalearn.config.JacksonConfig} 提供。
     */
    public static final class Holder {
        private Holder() {
        }
    }
}
