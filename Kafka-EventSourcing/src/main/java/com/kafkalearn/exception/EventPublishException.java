package com.kafkalearn.exception;

/**
 * 事件发布异常。
 */
public class EventPublishException extends RuntimeException {

    /**
     * 构造异常。
     *
     * @param message 错误信息
     */
    public EventPublishException(String message) {
        super(message);
    }

    /**
     * 构造异常。
     *
     * @param message 错误信息
     * @param cause   原始异常
     */
    public EventPublishException(String message, Throwable cause) {
        super(message, cause);
    }
}
