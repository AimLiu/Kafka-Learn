package com.kafkalearn.exception;

/**
 * 账户已关闭异常。
 */
public class AccountClosedException extends RuntimeException {

    /**
     * 构造异常。
     *
     * @param message 错误信息
     */
    public AccountClosedException(String message) {
        super(message);
    }
}
