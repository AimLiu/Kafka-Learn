package com.kafkalearn.server;

import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author: Mafeifei
 * @email: owntnow@163.com
 * @Date: 2026/6/12 22:53
 * @Description: 默认新建三个账户，并且初始化余额
 */

@Component
public class InMemoryAccountBalanceStore {

    private final Map<UUID, BigDecimal> balances = new ConcurrentHashMap<>();

    public InMemoryAccountBalanceStore() {
        balances.put(UUID.fromString("11111111-1111-1111-1111-111111111111"), new BigDecimal("100.00"));
        balances.put(UUID.fromString("22222222-2222-2222-2222-222222222222"), new BigDecimal("250.50"));
        balances.put(UUID.fromString("33333333-3333-3333-3333-333333333333"), BigDecimal.ZERO);
    }

    public Optional<BigDecimal> findBalance(UUID accountId) {
        return Optional.ofNullable(balances.get(accountId));
    }
}