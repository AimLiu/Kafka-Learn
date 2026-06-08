package com.kafkalearn;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Event Sourcing 学习模块启动类。
 */
@SpringBootApplication
public class KafkaEventSourcingApplication {

    /**
     * 应用入口。
     *
     * @param args 启动参数
     */
    public static void main(String[] args) {
        SpringApplication.run(KafkaEventSourcingApplication.class, args);
    }
}
