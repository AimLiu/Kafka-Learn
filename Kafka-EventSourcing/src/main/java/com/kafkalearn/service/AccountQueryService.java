package com.kafkalearn.service;

import com.kafkalearn.api.dto.BalanceResponse;
import com.kafkalearn.api.dto.TransactionResponse;
import com.kafkalearn.entity.EsAccountBalanceView;
import com.kafkalearn.entity.EsAccountTransactionView;
import com.kafkalearn.exception.AccountNotFoundException;
import com.kafkalearn.repository.EsAccountBalanceViewRepository;
import com.kafkalearn.repository.EsAccountRepository;
import com.kafkalearn.repository.EsAccountTransactionViewRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.UUID;

/**
 * 账户读侧查询服务（CQRS Query）。
 */
@Service
@RequiredArgsConstructor
public class AccountQueryService {

    private final EsAccountRepository accountRepository;
    private final EsAccountBalanceViewRepository balanceViewRepository;
    private final EsAccountTransactionViewRepository transactionViewRepository;

    /**
     * 查询账户余额读模型。
     *
     * @param accountId 账户 ID
     * @return 余额响应
     */
    @Transactional(readOnly = true)
    public BalanceResponse getBalance(UUID accountId) {
        ensureAccountExists(accountId);
        EsAccountBalanceView view = balanceViewRepository.findById(accountId)
                .orElseThrow(() -> new AccountNotFoundException("余额读模型尚未投影完成，请稍后重试。accountId=" + accountId));
        return new BalanceResponse(
                view.getAccountId(),
                view.getOwnerName(),
                view.getBalance(),
                view.getStatus(),
                view.getLastEventSequence(),
                view.getUpdatedAt()
        );
    }

    /**
     * 查询账户流水读模型。
     *
     * @param accountId 账户 ID
     * @return 流水列表
     */
    @Transactional(readOnly = true)
    public List<TransactionResponse> listTransactions(UUID accountId) {
        ensureAccountExists(accountId);
        return transactionViewRepository.findByAccountIdOrderByOccurredAtDesc(accountId).stream()
                .map(this::toTransactionResponse)
                .toList();
    }

    private void ensureAccountExists(UUID accountId) {
        if (!accountRepository.existsById(accountId)) {
            throw new AccountNotFoundException("账户不存在: " + accountId);
        }
    }

    private TransactionResponse toTransactionResponse(EsAccountTransactionView view) {
        return new TransactionResponse(
                view.getTxId(),
                view.getAccountId(),
                view.getEventId(),
                view.getEventType(),
                view.getAmount(),
                view.getBalanceAfter(),
                view.getOccurredAt()
        );
    }
}
