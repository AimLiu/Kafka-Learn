package com.kafkalearn.demo.domain;

public enum TaskStatus {
    CREATED,
    PUBLISHED,
    CONSUMING,
    SUCCESS,
    RETRY_WAITING,
    FAILED
}
