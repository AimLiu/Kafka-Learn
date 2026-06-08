package com.kafkalearn.exception;

/**
 * 乐观锁冲突异常。
 */
public class OptimisticLockConflictException extends RuntimeException {

    /**
     * 构造异常。
     *
     * @param message 错误信息
     */
    public OptimisticLockConflictException(String message) {
        super(message);
    }
}
