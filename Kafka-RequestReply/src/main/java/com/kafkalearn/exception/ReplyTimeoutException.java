package com.kafkalearn.exception;

/**
 * @author: Mafeifei
 * @email: owntnow@163.com
 * @Date: 2026/6/13 11:41
 * @Description:
 */

public class ReplyTimeoutException extends RuntimeException {
    public ReplyTimeoutException(String correlationId, long timeoutMs) {
        super("Reply timed out for correlationId=" + correlationId + ", timeoutMs=" + timeoutMs);
    }
}
