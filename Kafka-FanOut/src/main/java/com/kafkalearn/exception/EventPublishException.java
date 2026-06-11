package com.kafkalearn.exception;

import com.fasterxml.jackson.core.JsonProcessingException;

public class EventPublishException extends RuntimeException {
    public EventPublishException(String message) {
        super(message);
    }

    public EventPublishException(String message, Exception e) {
        super(message, e);
    }
}
