package com.kafkalearn;

import com.kafkalearn.config.SimulatorProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
@EnableConfigurationProperties(SimulatorProperties.class)
public class KafkaFanOutApplication {
    public static void main(String[] args) {
        SpringApplication.run(KafkaFanOutApplication.class, args);
    }
}
