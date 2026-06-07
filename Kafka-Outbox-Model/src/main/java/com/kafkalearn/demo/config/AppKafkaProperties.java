package com.kafkalearn.demo.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "app.kafka")
public class AppKafkaProperties {

    private String callbackTopic;
    private long sendTimeoutSeconds = 10L;

    public String getCallbackTopic() {
        return callbackTopic;
    }

    public void setCallbackTopic(String callbackTopic) {
        this.callbackTopic = callbackTopic;
    }

    public long getSendTimeoutSeconds() {
        return sendTimeoutSeconds;
    }

    public void setSendTimeoutSeconds(long sendTimeoutSeconds) {
        this.sendTimeoutSeconds = sendTimeoutSeconds;
    }
}
