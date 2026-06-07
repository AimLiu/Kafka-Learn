package com.kafkalearn.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.ConfigurationPropertiesBinding;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "app.kafka")
public class ProducerKafkaProperties {
    private String produceTopic;
    private Integer sendTimeoutSeconds;

    public String getProduceTopic() {
        return produceTopic;
    }

    public void setProduceTopic(String produceTopic) {
        this.produceTopic = produceTopic;
    }

    public void setSendTimeoutSeconds(Integer sendTimeoutSeconds) {
        this.sendTimeoutSeconds = sendTimeoutSeconds;
    }

    public Integer getSendTimeoutSeconds() {
        return sendTimeoutSeconds;
    }
}
