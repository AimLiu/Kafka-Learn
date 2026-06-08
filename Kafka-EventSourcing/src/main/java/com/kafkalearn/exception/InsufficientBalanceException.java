package com.kafkalearn.exception;

/**
 * 余额不足异常。
 */
public class InsufficientBalanceException extends RuntimeException {

    /**
     * 构造异常。
     *
     * @param message 错误信息
     */
    public InsufficientBalanceException(String message) {
        super(message);
    }
}
