package com.kafkalearn;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class KafkaEventDrivenApplication {
    public static void main(String[] args) {
        SpringApplication.run(KafkaEventDrivenApplication.class, args);
    }
}
