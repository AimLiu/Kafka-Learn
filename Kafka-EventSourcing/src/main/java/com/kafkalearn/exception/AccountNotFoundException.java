package com.kafkalearn.exception;

/**
 * 账户不存在异常。
 */
public class AccountNotFoundException extends RuntimeException {

    /**
     * 构造异常。
     *
     * @param message 错误信息
     */
    public AccountNotFoundException(String message) {
        super(message);
    }
}
