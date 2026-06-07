package com.kafkalearn.demo.domain;

public enum OutboxStatus {
    NEW,
    PUBLISHED,
    PUBLISH_FAILED
}
