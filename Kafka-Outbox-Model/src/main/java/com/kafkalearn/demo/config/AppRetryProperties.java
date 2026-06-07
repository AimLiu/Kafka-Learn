package com.kafkalearn.demo.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "app.retry")
public class AppRetryProperties {

    private int maxRetryCount = 3;
    private long firstDelaySeconds = 30;
    private long secondDelaySeconds = 60;
    private long thirdDelaySeconds = 180;
    private long consumeTimeoutSeconds = 120;

    public int getMaxRetryCount() {
        return maxRetryCount;
    }

    public void setMaxRetryCount(int maxRetryCount) {
        this.maxRetryCount = maxRetryCount;
    }

    public long getFirstDelaySeconds() {
        return firstDelaySeconds;
    }

    public void setFirstDelaySeconds(long firstDelaySeconds) {
        this.firstDelaySeconds = firstDelaySeconds;
    }

    public long getSecondDelaySeconds() {
        return secondDelaySeconds;
    }

    public void setSecondDelaySeconds(long secondDelaySeconds) {
        this.secondDelaySeconds = secondDelaySeconds;
    }

    public long getThirdDelaySeconds() {
        return thirdDelaySeconds;
    }

    public void setThirdDelaySeconds(long thirdDelaySeconds) {
        this.thirdDelaySeconds = thirdDelaySeconds;
    }

    public long getConsumeTimeoutSeconds() {
        return consumeTimeoutSeconds;
    }

    public void setConsumeTimeoutSeconds(long consumeTimeoutSeconds) {
        this.consumeTimeoutSeconds = consumeTimeoutSeconds;
    }
}
