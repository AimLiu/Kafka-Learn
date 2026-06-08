package com.kafkalearn.api;

import com.kafkalearn.api.dto.CloseAccountRequest;
import com.kafkalearn.api.dto.CommandResponse;
import com.kafkalearn.api.dto.DepositRequest;
import com.kafkalearn.api.dto.OpenAccountRequest;
import com.kafkalearn.api.dto.WithdrawRequest;
import com.kafkalearn.service.AccountCommandService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

/**
 * 账户写命令 API。
 */
@RestController
@RequestMapping("/api/accounts")
@RequiredArgsConstructor
public class AccountCommandController {

    private final AccountCommandService commandService;

    /**
     * 开户。
     *
     * @param request 开户请求
     * @return 命令响应
     */
    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public CommandResponse openAccount(@Valid @RequestBody OpenAccountRequest request) {
        return commandService.openAccount(request);
    }

    /**
     * 充值。
     *
     * @param accountId 账户 ID
     * @param request   充值请求
     * @return 命令响应
     */
    @PostMapping("/{accountId}/deposit")
    public CommandResponse deposit(@PathVariable UUID accountId,
                                   @Valid @RequestBody DepositRequest request) {
        return commandService.deposit(accountId, request);
    }

    /**
     * 扣款。
     *
     * @param accountId 账户 ID
     * @param request   扣款请求
     * @return 命令响应
     */
    @PostMapping("/{accountId}/withdraw")
    public CommandResponse withdraw(@PathVariable UUID accountId,
                                    @Valid @RequestBody WithdrawRequest request) {
        return commandService.withdraw(accountId, request);
    }

    /**
     * 关户。
     *
     * @param accountId 账户 ID
     * @param request   关户请求
     * @return 命令响应
     */
    @PostMapping("/{accountId}/close")
    public CommandResponse closeAccount(@PathVariable UUID accountId,
                                        @Valid @RequestBody CloseAccountRequest request) {
        return commandService.closeAccount(accountId, request);
    }
}
