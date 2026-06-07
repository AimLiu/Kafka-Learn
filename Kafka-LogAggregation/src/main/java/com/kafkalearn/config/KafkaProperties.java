package com.kafkalearn.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * @author: Mafeifei
 * @email: owntnow@163.com
 * @Date: 2026/6/7 16:04
 * @Description:
 */

@Component
@ConfigurationProperties(prefix = "app.kafka")
public class KafkaProperties {
    private String produceTopic;
    private Integer sendTimeoutSeconds;

    public String getProduceTopic() {
        return produceTopic;
    }

    public void setProduceTopic(String produceTopic) {
        this.produceTopic = produceTopic;
    }

    public Integer getSendTimeoutSeconds() {
        return sendTimeoutSeconds;
    }

    public void setSendTimeoutSeconds(Integer sendTimeoutSeconds) {
        this.sendTimeoutSeconds = sendTimeoutSeconds;
    }
}

