package com.kafkalearn.exception;

public class AlertProcessException extends RuntimeException {
    public AlertProcessException(String message) {
        super(message);
    }
    public AlertProcessException(String message, Throwable cause) {
        super(message, cause);
    }
}
