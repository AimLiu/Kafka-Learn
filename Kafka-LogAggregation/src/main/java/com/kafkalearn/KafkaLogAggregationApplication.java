package com.kafkalearn;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class KafkaLogAggregationApplication {

    public static void main(String[] args) {
        SpringApplication.run(KafkaLogAggregationApplication.class, args);
    }
}
