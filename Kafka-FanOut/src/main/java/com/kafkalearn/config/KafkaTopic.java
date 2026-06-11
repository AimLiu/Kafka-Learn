package com.kafkalearn.config;

public interface KafkaTopic {
    String DEVICE_ALERT_TOPIC = "device-alert-topic";
    String DEVICE_ALERT_STORAGE_DLQ_TOPIC = "device-alert-storage-dlq";
    String DEVICE_ALERT_PUSH_DLQ_TOPIC = "device-alert-push-dlq";
    String DEVICE_ALERT_RULE_DLQ_TOPIC = "device-alert-rule-dlq";
}
