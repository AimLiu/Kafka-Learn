package com.kafkalearn.demo.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "app.scheduler")
public class AppSchedulerProperties {

    private long outboxDelayMs = 5000;
    private long retryDelayMs = 10000;
    private long recoveryDelayMs = 15000;
    private int batchSize = 20;

    public long getOutboxDelayMs() {
        return outboxDelayMs;
    }

    public void setOutboxDelayMs(long outboxDelayMs) {
        this.outboxDelayMs = outboxDelayMs;
    }

    public long getRetryDelayMs() {
        return retryDelayMs;
    }

    public void setRetryDelayMs(long retryDelayMs) {
        this.retryDelayMs = retryDelayMs;
    }

    public long getRecoveryDelayMs() {
        return recoveryDelayMs;
    }

    public void setRecoveryDelayMs(long recoveryDelayMs) {
        this.recoveryDelayMs = recoveryDelayMs;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }
}
