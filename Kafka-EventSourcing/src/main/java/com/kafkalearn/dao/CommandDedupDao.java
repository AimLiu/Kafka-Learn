package com.kafkalearn.dao;

import com.kafkalearn.entity.EsCommandDedup;
import com.kafkalearn.exception.CommandInProgressException;
import com.kafkalearn.exception.DuplicateCommandException;
import com.kafkalearn.repository.EsCommandDedupRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

/**
 * 命令幂等 DAO。
 */
@Component
@RequiredArgsConstructor
public class CommandDedupDao {

    private final EsCommandDedupRepository dedupRepository;

    /**
     * 查询已成功处理的命令记录。
     *
     * @param commandId 命令 ID
     * @return 成功记录
     */
    @Transactional(readOnly = true)
    public Optional<EsCommandDedup> findSucceeded(UUID commandId) {
        return dedupRepository.findById(commandId)
                .filter(record -> record.getStatus() == EsCommandDedup.CommandStatus.SUCCEEDED);
    }

    /**
     * 尝试登记命令为处理中。
     *
     * @param commandId   命令 ID
     * @param commandType 命令类型
     * @param aggregateId 聚合 ID，开户时可为 null
     * @return 新登记记录；若已存在则抛出相应异常
     */
    @Transactional
    public EsCommandDedup beginProcessing(UUID commandId, String commandType, UUID aggregateId) {
        Optional<EsCommandDedup> existing = dedupRepository.findById(commandId);
        //判断命令是否已处理
        if (existing.isPresent()) {
            EsCommandDedup record = existing.get();
            if (record.getStatus() == EsCommandDedup.CommandStatus.SUCCEEDED) {
                throw new DuplicateCommandException(record);
            }
            if (record.getStatus() == EsCommandDedup.CommandStatus.PROCESSING) {
                throw new CommandInProgressException("命令正在处理中: " + commandId);
            }
            record.setStatus(EsCommandDedup.CommandStatus.PROCESSING);
            record.setUpdatedAt(Instant.now());
            record.setErrorMessage(null);
            return dedupRepository.save(record);
        }
        EsCommandDedup record = new EsCommandDedup();
        record.setCommandId(commandId);
        record.setCommandType(commandType);
        record.setAggregateId(aggregateId);
        record.setStatus(EsCommandDedup.CommandStatus.PROCESSING);
        Instant now = Instant.now();
        record.setCreatedAt(now);
        record.setUpdatedAt(now);
        try {
            return dedupRepository.save(record);
        } catch (DataIntegrityViolationException ex) {
            EsCommandDedup found = dedupRepository.findById(commandId).orElseThrow(() -> ex);
            if (found.getStatus() == EsCommandDedup.CommandStatus.SUCCEEDED) {
                throw new DuplicateCommandException(found);
            }
            throw new CommandInProgressException("命令正在处理中: " + commandId);
        }
    }

    /**
     * 标记命令处理成功。
     *
     * @param commandId     命令 ID
     * @param aggregateId   聚合 ID
     * @param resultEventId 结果事件 ID
     */
    @Transactional
    public void markSucceeded(UUID commandId, UUID aggregateId, UUID resultEventId) {
        EsCommandDedup record = dedupRepository.findById(commandId)
                .orElseThrow(() -> new IllegalStateException("命令记录不存在: " + commandId));
        record.setStatus(EsCommandDedup.CommandStatus.SUCCEEDED);
        record.setAggregateId(aggregateId);
        record.setResultEventId(resultEventId);
        record.setUpdatedAt(Instant.now());
        dedupRepository.save(record);
    }

    /**
     * 标记命令处理失败。
     *
     * @param commandId    命令 ID
     * @param errorMessage 错误信息
     */
    @Transactional
    public void markFailed(UUID commandId, String errorMessage) {
        dedupRepository.findById(commandId).ifPresent(record -> {
            record.setStatus(EsCommandDedup.CommandStatus.FAILED);
            record.setErrorMessage(errorMessage);
            record.setUpdatedAt(Instant.now());
            dedupRepository.save(record);
        });
    }
}
