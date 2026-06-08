package com.kafkalearn.exception;

/**
 * 命令处理中异常。
 */
public class CommandInProgressException extends RuntimeException {

    /**
     * 构造异常。
     *
     * @param message 错误信息
     */
    public CommandInProgressException(String message) {
        super(message);
    }
}
