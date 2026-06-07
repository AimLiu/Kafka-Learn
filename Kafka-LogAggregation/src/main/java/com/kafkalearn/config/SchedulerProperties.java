package com.kafkalearn.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "app.scheduler")
public class SchedulerProperties {
    private long sendDelayMs = 5000;

    public long getSendDelayMs() {
        return sendDelayMs;
    }

    public void setSendDelayMs(long sendDelayMs) {
        this.sendDelayMs = sendDelayMs;
    }
}
