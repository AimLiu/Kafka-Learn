package com.kafkalearn.service;

import com.kafkalearn.domain.AccountStatus;
import com.kafkalearn.domain.event.AccountEventType;
import com.kafkalearn.entity.EsAccountBalanceView;
import com.kafkalearn.entity.EsAccountTransactionView;
import com.kafkalearn.messaging.AccountDomainEventMessage;
import com.kafkalearn.repository.EsAccountBalanceViewRepository;
import com.kafkalearn.repository.EsAccountTransactionViewRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * 账户读模型投影服务：将领域事件应用到 CQRS 读表。
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class AccountProjectionService {

    private final EsAccountBalanceViewRepository balanceViewRepository;
    private final EsAccountTransactionViewRepository transactionViewRepository;

    /**
     * 投影单条账户领域事件（幂等）。
     *
     * @param message 领域事件消息
     */
    @Transactional
    public void project(AccountDomainEventMessage message) {
        if (transactionViewRepository.existsByEventId(message.eventId())) {
            log.debug("事件已投影，跳过。eventId=[{}]", message.eventId());
            return;
        }
        EsAccountBalanceView balanceView = balanceViewRepository.findById(message.aggregateId())
                .orElseGet(() -> createBalanceView(message));
        applyEvent(balanceView, message);
        balanceView.setLastEventSequence(message.sequence());
        balanceView.setUpdatedAt(Instant.now());
        balanceViewRepository.save(balanceView);
        persistTransactionIfNeeded(balanceView, message);
        log.info("读模型投影完成, accountId=[{}], eventType=[{}], sequence=[{}]",
                message.aggregateId(), message.eventType(), message.sequence());
    }

    private EsAccountBalanceView createBalanceView(AccountDomainEventMessage message) {
        EsAccountBalanceView view = new EsAccountBalanceView();
        view.setAccountId(message.aggregateId());
        view.setBalance(BigDecimal.ZERO);
        view.setStatus(AccountStatus.ACTIVE);
        view.setLastEventSequence(0);
        view.setUpdatedAt(Instant.now());
        if (AccountEventType.ACCOUNT_OPENED.equals(message.eventType())) {
            view.setOwnerName(message.payload().get("ownerName").asText());
            view.setBalance(message.payload().get("initialBalance").decimalValue());
        }
        return view;
    }

    /**
     * 根据事件类型，执行对应操作
     * @param view
     * @param message
     */
    private void applyEvent(EsAccountBalanceView view, AccountDomainEventMessage message) {
        switch (message.eventType()) {
            case AccountEventType.ACCOUNT_OPENED -> {
                view.setOwnerName(message.payload().get("ownerName").asText());
                view.setBalance(message.payload().get("initialBalance").decimalValue());
                view.setStatus(AccountStatus.ACTIVE);
            }
            case AccountEventType.MONEY_DEPOSITED ->
                    view.setBalance(view.getBalance().add(message.payload().get("amount").decimalValue()));
            case AccountEventType.MONEY_WITHDRAWN ->
                    view.setBalance(view.getBalance().subtract(message.payload().get("amount").decimalValue()));
            case AccountEventType.ACCOUNT_CLOSED -> view.setStatus(AccountStatus.CLOSED);
            default -> throw new IllegalStateException("未知事件类型: " + message.eventType());
        }
    }

    private void persistTransactionIfNeeded(EsAccountBalanceView balanceView, AccountDomainEventMessage message) {
        BigDecimal amount = resolveTransactionAmount(message);
        if (amount == null) {
            return;
        }
        EsAccountTransactionView tx = new EsAccountTransactionView();
        tx.setTxId(UUID.randomUUID());
        tx.setAccountId(message.aggregateId());
        tx.setEventId(message.eventId());
        tx.setEventType(message.eventType());
        tx.setAmount(amount);
        tx.setBalanceAfter(balanceView.getBalance());
        tx.setOccurredAt(message.occurredAt() != null ? message.occurredAt() : Instant.now());
        transactionViewRepository.save(tx);
    }

    private BigDecimal resolveTransactionAmount(AccountDomainEventMessage message) {
        return switch (message.eventType()) {
            case AccountEventType.ACCOUNT_OPENED -> message.payload().get("initialBalance").decimalValue();
            case AccountEventType.MONEY_DEPOSITED, AccountEventType.MONEY_WITHDRAWN ->
                    message.payload().get("amount").decimalValue();
            case AccountEventType.ACCOUNT_CLOSED -> null;
            default -> null;
        };
    }
}
