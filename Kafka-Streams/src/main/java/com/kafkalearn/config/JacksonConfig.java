package com.kafkalearn.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Jackson ObjectMapper 配置。
 */
@Configuration
public class JacksonConfig {

    /**
     * 提供全局 ObjectMapper，供 Codec 与 JsonSerde 共用。
     *
     * @return ObjectMapper 实例
     */
    @Bean
    public ObjectMapper objectMapper() {
        return new ObjectMapper();
    }
}
