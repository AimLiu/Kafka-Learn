package com.kafkalearn.dao;

import com.kafkalearn.domain.AccountAggregate;
import com.kafkalearn.entity.EsAccount;
import com.kafkalearn.entity.EsEvent;
import com.kafkalearn.exception.AccountNotFoundException;
import com.kafkalearn.exception.OptimisticLockConflictException;
import com.kafkalearn.repository.EsAccountRepository;
import com.kafkalearn.repository.EsEventRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.UUID;

/**
 * 账户聚合加载 DAO：从元数据与事件流重建 {@link AccountAggregate}。
 */
@Component
@RequiredArgsConstructor
public class AccountLoadDao {

    private final EsAccountRepository accountRepository;
    private final EsEventRepository eventRepository;

    /**
     * 加载并重建账户聚合。
     *
     * @param accountId 账户 ID
     * @return 聚合根
     */
    @Transactional(readOnly = true)
    public AccountAggregate load(UUID accountId) {
        EsAccount account = accountRepository.findById(accountId)
                .orElseThrow(() -> new AccountNotFoundException("账户不存在: " + accountId));
        List<EsEvent> events = eventRepository.findByAggregateIdOrderBySequenceAsc(accountId);
        return AccountAggregate.replay(account, events);
    }

    /**
     * 加载账户元数据。
     *
     * @param accountId 账户 ID
     * @return 账户元数据
     */
    @Transactional(readOnly = true)
    public EsAccount loadAccountMeta(UUID accountId) {
        return accountRepository.findById(accountId)
                .orElseThrow(() -> new AccountNotFoundException("账户不存在: " + accountId));
    }
}
