package com.kafkalearn.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.UUID;

@Data
@ConfigurationProperties(prefix = "app.simulator")
public class SimulatorProperties {

    private long randomIntervalMs = 5000;
    private boolean learningCycleEnabled = true;
    private long learningCycleIntervalMs = 30000;
    private UUID fixedDeviceId = UUID.fromString("c542567a-dfb1-4af7-940a-a5cada4372b6");
}
