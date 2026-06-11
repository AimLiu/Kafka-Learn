package com.kafkalearn.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * Kafka 配置属性类
 *
 * 用于从 application.yml 或 Nacos 配置中读取 Kafka 相关配置
 *
 * @author Mafei
 * @since 1.0.0
 */

@Data
@Component
@ConfigurationProperties(prefix = "spring.kafka")
public class KafkaProperties {
    /**
     * Kafka Bootstrap 服务器地址
     * 格式: localhost:9092 或 kafka1:9092,kafka2:9092,kafka3:9092
     */
    private String bootstrapServers = "localhost:9092";

    /**
     * Producer 配置
     */
    private ProducerConfig producer = new ProducerConfig();

    /**
     * Consumer 配置
     */
    private ConsumerConfig consumer = new ConsumerConfig();

    /**
     * Topic 配置
     */
    private TopicConfig topic = new TopicConfig();

    @Data
    public static class ProducerConfig {
        /**
         * 生产者确认级别
         * - 0: 不等待任何确认
         * - 1: 等待 leader 确认
         * - all: 等待所有副本确认
         */
        private String acks = "all";

        /**
         * 重试次数
         */
        private Integer retries = 3;

        /**
         * 批处理大小（字节）
         */
        private Integer batchSize = 16384;

        /**
         * 等待时间（毫秒）
         */
        private Integer lingerMs = 10;

        /**
         * 压缩类型: none, snappy, lz4, zstd
         */
        private String compressionType = "snappy";

        /**
         * 请求超时时间（毫秒）
         */
        private Integer requestTimeoutMs = 30000;

        /**
         * 生产者幂等
         */
        private boolean enableIdempotence = true;
    }

    @Data
    public class ConsumerConfig {
        /**
         * 消费者组 ID
         */
        private String groupId = "default-group";

        /**
         * 自动提交偏移量
         */
        private Boolean enableAutoCommit = true;

        /**
         * 自动提交间隔（毫秒）
         */
        private Integer autoCommitIntervalMs = 1000;

        /**
         * 自动重置偏移量策略
         * - earliest: 从最早的消息开始
         * - latest: 从最新的消息开始
         * - none: 如果没有找到消费者组，抛出异常
         */
        private String autoOffsetReset = "earliest";

        /**
         * 单次拉取的最大记录数
         */
        private Integer maxPollRecords = 500;

        /**
         * 会话超时时间（毫秒）
         */
        private Integer sessionTimeoutMs = 30000;

        /**
         * 心跳间隔（毫秒）
         */
        private Integer heartbeatIntervalMs = 10000;
    }

    @Data
    public static class TopicConfig {
        /**
         * 分区数
         */
        private Integer partitions = 3;

        /**
         * 副本因子
         */
        private Short replicationFactor = 1;

        /**
         * 消息保留时间（毫秒）
         * -1 表示无限期保留
         */
        private Long retentionMs = -1L;

        /**
         * 消息保留大小（字节）
         * -1 表示无限制
         */
        private Long retentionBytes = -1L;
    }
}
