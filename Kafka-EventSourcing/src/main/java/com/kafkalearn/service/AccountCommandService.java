package com.kafkalearn.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafkalearn.api.dto.CloseAccountRequest;
import com.kafkalearn.api.dto.CommandResponse;
import com.kafkalearn.api.dto.DepositRequest;
import com.kafkalearn.api.dto.OpenAccountRequest;
import com.kafkalearn.api.dto.WithdrawRequest;
import com.kafkalearn.dao.AccountLoadDao;
import com.kafkalearn.dao.CommandDedupDao;
import com.kafkalearn.dao.EventStoreDao;
import com.kafkalearn.domain.AccountAggregate;
import com.kafkalearn.entity.EsAccount;
import com.kafkalearn.entity.EsCommandDedup;
import com.kafkalearn.entity.EsEvent;
import com.kafkalearn.exception.OptimisticLockConflictException;
import com.kafkalearn.messaging.AccountDomainEventMessage;
import com.kafkalearn.producer.AccountEventPublisher;
import com.kafkalearn.repository.EsEventRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.util.UUID;
import java.util.function.Function;

/**
 * 账户写侧命令服务（CQRS Command）。
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class AccountCommandService {

    private static final String CMD_OPEN = "OpenAccount";
    private static final String CMD_DEPOSIT = "Deposit";
    private static final String CMD_WITHDRAW = "Withdraw";
    private static final String CMD_CLOSE = "CloseAccount";

    private final ObjectMapper objectMapper;
    private final CommandDedupDao commandDedupDao;
    private final EventStoreDao eventStoreDao;
    private final AccountLoadDao accountLoadDao;
    private final AccountEventPublisher eventPublisher;
    private final EsEventRepository eventRepository;

    /**
     * 开户命令。
     *
     * @param request 开户请求
     * @return 命令响应
     */
    @Transactional
    public CommandResponse openAccount(OpenAccountRequest request) {
        CommandResponse idempotent = tryIdempotentResponse(request.commandId());
        if (idempotent != null) {
            return idempotent;
        }
        // 记录命令处理状态
        commandDedupDao.beginProcessing(request.commandId(), CMD_OPEN, null);
        try {
            UUID accountId = UUID.randomUUID();
            AccountAggregate.PendingEvent pending = AccountAggregate.open(
                    accountId,
                    request.ownerName(),
                    request.initialBalance(),
                    request.commandId(),
                    objectMapper
            );
            EsAccount account = eventStoreDao.buildNewAccount(accountId, request.ownerName(), pending);
            EsEvent savedEvent = eventStoreDao.saveOpenAccount(account, pending);
            commandDedupDao.markSucceeded(request.commandId(), accountId, savedEvent.getEventId());
            publishAfterCommit(savedEvent);
            return new CommandResponse(accountId, savedEvent.getEventId(), savedEvent.getSequence(), false);
        } catch (RuntimeException ex) {
            commandDedupDao.markFailed(request.commandId(), ex.getMessage());
            throw ex;
        }
    }

    /**
     * 充值命令。
     *
     * @param accountId 账户 ID
     * @param request   充值请求
     * @return 命令响应
     */
    @Transactional
    public CommandResponse deposit(UUID accountId, DepositRequest request) {
        return mutateAccount(accountId, request.commandId(), CMD_DEPOSIT, request.expectedVersion(), aggregate ->
                aggregate.deposit(request.amount(), request.commandId(), objectMapper), false);
    }

    /**
     * 扣款命令。
     *
     * @param accountId 账户 ID
     * @param request   扣款请求
     * @return 命令响应
     */
    @Transactional
    public CommandResponse withdraw(UUID accountId, WithdrawRequest request) {
        return mutateAccount(accountId, request.commandId(), CMD_WITHDRAW, request.expectedVersion(), aggregate ->
                aggregate.withdraw(request.amount(), request.commandId(), objectMapper), false);
    }

    /**
     * 关户命令。
     *
     * @param accountId 账户 ID
     * @param request   关户请求
     * @return 命令响应
     */
    @Transactional
    public CommandResponse closeAccount(UUID accountId, CloseAccountRequest request) {
        String reason = request.reason() == null ? "用户申请关户" : request.reason();
        return mutateAccount(accountId, request.commandId(), CMD_CLOSE, request.expectedVersion(), aggregate ->
                aggregate.close(request.commandId(), reason, objectMapper), true);
    }

    private CommandResponse mutateAccount(UUID accountId,
                                        UUID commandId,
                                        String commandType,
                                        Long expectedVersion,
                                        Function<AccountAggregate, AccountAggregate.PendingEvent> mutator,
                                        boolean closing) {
        CommandResponse idempotent = tryIdempotentResponse(commandId);
        if (idempotent != null) {
            return idempotent;
        }
        commandDedupDao.beginProcessing(commandId, commandType, accountId);
        try {
            EsAccount meta = accountLoadDao.loadAccountMeta(accountId);
            validateExpectedVersion(meta.getCurrentVersion(), expectedVersion);
            AccountAggregate aggregate = accountLoadDao.load(accountId);
            AccountAggregate.PendingEvent pending = mutator.apply(aggregate);
            EsEvent savedEvent = eventStoreDao.appendEvent(meta.getCurrentVersion(), pending, closing);
            commandDedupDao.markSucceeded(commandId, accountId, savedEvent.getEventId());
            publishAfterCommit(savedEvent);
            return new CommandResponse(accountId, savedEvent.getEventId(), savedEvent.getSequence(), false);
        } catch (RuntimeException ex) {
            commandDedupDao.markFailed(commandId, ex.getMessage());
            throw ex;
        }
    }

    private void validateExpectedVersion(long actualVersion, Long expectedVersion) {
        if (expectedVersion != null && actualVersion != expectedVersion) {
            throw new OptimisticLockConflictException(
                    "账户版本不匹配，actual=" + actualVersion + ", expected=" + expectedVersion);
        }
    }

    private CommandResponse tryIdempotentResponse(UUID commandId) {
        return commandDedupDao.findSucceeded(commandId)
                .map(this::toIdempotentResponse)
                .orElse(null);
    }

    private CommandResponse toIdempotentResponse(EsCommandDedup record) {
        long version = eventRepository.findById(record.getResultEventId())
                .map(EsEvent::getSequence)
                .orElse(0L);
        return new CommandResponse(record.getAggregateId(), record.getResultEventId(), version, true);
    }

    private void publishAfterCommit(EsEvent savedEvent) {
        AccountDomainEventMessage message = AccountDomainEventMessage.from(savedEvent);
        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override
                public void afterCommit() {
                    eventPublisher.publish(message);
                }
            });
        } else {
            eventPublisher.publish(message);
        }
    }
}
