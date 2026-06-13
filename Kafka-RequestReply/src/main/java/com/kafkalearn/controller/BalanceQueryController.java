package com.kafkalearn.controller;

import com.kafkalearn.client.BalanceQueryClient;
import com.kafkalearn.message.ReplyStatus;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

/**
 * @author: Mafeifei
 * @email: owntnow@163.com
 * @Date: 2026/6/13 11:46
 * @Description:
 */

@RestController
@RequiredArgsConstructor
public class BalanceQueryController {

    private final BalanceQueryClient balanceQueryClient;

    @GetMapping("/api/accounts/{accountId}/balance")
    public ResponseEntity<Map<String, Object>> queryBalance(
            @PathVariable("accountId") UUID accountId,
            @RequestParam(value = "simulateDelayMs", defaultValue = "0") long simulateDelayMs) {

        var result = balanceQueryClient.query(accountId, simulateDelayMs);
        // 通过future拿到response结果
        var response = result.response();

        Map<String, Object> body = new LinkedHashMap<>();
        body.put("accountId", response.accountId());
        body.put("balance", response.balance());
        body.put("status", response.status());
        body.put("message", response.message());
        body.put("correlationId", result.correlationId());
        body.put("elapsedMs", result.elapsedMs());

        if (response.status() == ReplyStatus.NOT_FOUND) {
            return ResponseEntity.status(200).body(body);
        }
        return ResponseEntity.ok(body);
    }
}