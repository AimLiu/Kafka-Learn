package com.kafkalearn.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * 从 {@code application.yml} 读取 Kafka 连接与 Producer/Consumer 参数。
 */
@Data
@Component
@ConfigurationProperties(prefix = "spring.kafka")
public class KafkaProperties {

    /** Kafka Bootstrap 地址。 */
    private String bootstrapServers = "localhost:9092";

    /** Producer 配置。 */
    private ProducerConfig producer = new ProducerConfig();

    /** Consumer 配置。 */
    private ConsumerConfig consumer = new ConsumerConfig();

    /**
     * Kafka Producer 相关配置。
     */
    @Data
    public static class ProducerConfig {

        /** 确认级别：all 表示 ISR 全部确认。 */
        private String acks = "all";

        /** 发送失败重试次数。 */
        private Integer retries = 3;

        /** 批处理大小（字节）。 */
        private Integer batchSize = 16384;

        /** 凑批等待时间（毫秒）。 */
        private Integer lingerMs = 10;

        /** 压缩算法。 */
        private String compressionType = "snappy";

        /** 请求超时（毫秒）。 */
        private Integer requestTimeoutMs = 30000;

        /** 是否启用幂等 Producer。 */
        private boolean enableIdempotence = true;
    }

    /**
     * Kafka Consumer 相关配置（供 @KafkaListener 使用）。
     */
    @Data
    public static class ConsumerConfig {

        /** 无 offset 时的起始策略。 */
        private String autoOffsetReset = "earliest";

        /** 单次 poll 最大记录数。 */
        private Integer maxPollRecords = 500;

        /** 是否自动提交 offset。 */
        private Boolean enableAutoCommit = false;
    }
}
