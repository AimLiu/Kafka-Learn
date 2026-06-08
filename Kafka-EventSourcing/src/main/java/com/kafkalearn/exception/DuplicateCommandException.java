package com.kafkalearn.exception;

import com.kafkalearn.entity.EsCommandDedup;
import lombok.Getter;

/**
 * 重复命令异常（幂等命中）。
 */
@Getter
public class DuplicateCommandException extends RuntimeException {

    private final EsCommandDedup existingRecord;

    /**
     * 构造异常。
     *
     * @param existingRecord 已存在的命令记录
     */
    public DuplicateCommandException(EsCommandDedup existingRecord) {
        super("命令已处理: " + existingRecord.getCommandId());
        this.existingRecord = existingRecord;
    }
}
