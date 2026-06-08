package com.kafkalearn.api;

import com.kafkalearn.api.dto.BalanceResponse;
import com.kafkalearn.api.dto.TransactionResponse;
import com.kafkalearn.service.AccountQueryService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.UUID;

/**
 * 账户读查询 API。
 */
@RestController
@RequestMapping("/api/accounts")
@RequiredArgsConstructor
public class AccountQueryController {

    private final AccountQueryService queryService;

    /**
     * 查询账户余额。
     *
     * @param accountId 账户 ID
     * @return 余额响应
     */
    @GetMapping("/{accountId}/balance")
    public BalanceResponse getBalance(@PathVariable UUID accountId) {
        return queryService.getBalance(accountId);
    }

    /**
     * 查询账户流水。
     *
     * @param accountId 账户 ID
     * @return 流水列表
     */
    @GetMapping("/{accountId}/transactions")
    public List<TransactionResponse> listTransactions(@PathVariable UUID accountId) {
        return queryService.listTransactions(accountId);
    }
}
