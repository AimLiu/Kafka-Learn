package com.kafkalearn;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Kafka Streams 学习模块启动类。
 *
 * <p>同时启用 {@link EnableKafka}（KafkaTemplate / @KafkaListener）
 * 与 {@link EnableKafkaStreams}（Topology 流处理）。
 */
@SpringBootApplication
@EnableKafka
@EnableKafkaStreams
@EnableScheduling
public class KafkaStreamsApplication {

    /**
     * 应用入口。
     *
     * @param args 启动参数
     */
    public static void main(String[] args) {
        SpringApplication.run(KafkaStreamsApplication.class, args);
    }
}
