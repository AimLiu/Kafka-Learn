package com.kafkalearn.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * 账户流水读模型实体，对应表 {@code es_account_transaction_view}。
 */
@Getter
@Setter
@NoArgsConstructor
@Entity
@Table(name = "es_account_transaction_view")
public class EsAccountTransactionView {

    @Id
    @Column(name = "tx_id", nullable = false)
    private UUID txId;

    @Column(name = "account_id", nullable = false)
    private UUID accountId;

    @Column(name = "event_id", nullable = false)
    private UUID eventId;

    @Column(name = "event_type", nullable = false, length = 64)
    private String eventType;

    @Column(name = "amount", nullable = false, precision = 18, scale = 2)
    private BigDecimal amount;

    @Column(name = "balance_after", nullable = false, precision = 18, scale = 2)
    private BigDecimal balanceAfter;

    @Column(name = "occurred_at", nullable = false)
    private Instant occurredAt;
}
