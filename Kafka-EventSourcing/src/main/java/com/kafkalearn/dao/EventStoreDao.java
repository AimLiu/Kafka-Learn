package com.kafkalearn.dao;

import com.kafkalearn.domain.AccountAggregate;
import com.kafkalearn.domain.AccountStatus;
import com.kafkalearn.entity.EsAccount;
import com.kafkalearn.entity.EsEvent;
import com.kafkalearn.exception.OptimisticLockConflictException;
import com.kafkalearn.repository.EsAccountRepository;
import com.kafkalearn.repository.EsEventRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;

/**
 * 事件存储 DAO：负责向 {@code es_event} 追加事件并维护乐观锁版本。
 */
@Component
@RequiredArgsConstructor
public class EventStoreDao {

    private final EsAccountRepository accountRepository;
    private final EsEventRepository eventRepository;

    /**
     * 持久化开户聚合与首条事件。
     *
     * @param account 新账户元数据
     * @param event   开户事件
     * @return 已持久化事件
     */
    @Transactional
    public EsEvent saveOpenAccount(EsAccount account, AccountAggregate.PendingEvent event) {
        accountRepository.save(account);
        return persistEvent(event);
    }

    /**
     * 在乐观锁保护下追加事件（充值、扣款、关户）。
     *
     * @param expectedVersion 期望的当前版本
     * @param pendingEvent    待追加事件
     * @param closing         是否为关户事件
     * @return 已持久化事件
     */
    @Transactional
    public EsEvent appendEvent(long expectedVersion,
                               AccountAggregate.PendingEvent pendingEvent,
                               boolean closing) {
        int affected = closing
                ? accountRepository.closeAccount(pendingEvent.aggregateId(), expectedVersion)
                : accountRepository.incrementVersion(pendingEvent.aggregateId(), expectedVersion);
        if (affected == 0) {
            throw new OptimisticLockConflictException(
                    "账户版本冲突，请重试。accountId=" + pendingEvent.aggregateId() + ", expectedVersion=" + expectedVersion);
        }
        return persistEvent(pendingEvent);
    }

    private EsEvent persistEvent(AccountAggregate.PendingEvent pending) {
        EsEvent entity = new EsEvent();
        entity.setEventId(pending.eventId());
        entity.setAggregateType(pending.aggregateType());
        entity.setAggregateId(pending.aggregateId());
        entity.setEventType(pending.eventType());
        entity.setEventVersion(1);
        entity.setSequence(pending.sequence());
        entity.setCommandId(pending.commandId());
        entity.setPayload(pending.payload());
        entity.setOccurredAt(pending.occurredAt() != null ? pending.occurredAt() : Instant.now());
        return eventRepository.save(entity);
    }

    /**
     * 构建新账户元数据实体。
     *
     * @param accountId    账户 ID
     * @param ownerName    户名
     * @param initialEvent 首条事件
     * @return 账户元数据
     */
    public EsAccount buildNewAccount(java.util.UUID accountId, String ownerName, AccountAggregate.PendingEvent initialEvent) {
        Instant now = Instant.now();
        EsAccount account = new EsAccount();
        account.setAccountId(accountId);
        account.setOwnerName(ownerName);
        account.setStatus(AccountStatus.ACTIVE);
        account.setCurrentVersion(initialEvent.sequence());
        account.setCreatedAt(now);
        account.setUpdatedAt(now);
        return account;
    }
}
