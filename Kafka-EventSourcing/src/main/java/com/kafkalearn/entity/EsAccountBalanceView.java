package com.kafkalearn.entity;

import com.kafkalearn.domain.AccountStatus;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * 账户余额读模型实体，对应表 {@code es_account_balance_view}。
 */
@Getter
@Setter
@NoArgsConstructor
@Entity
@Table(name = "es_account_balance_view")
public class EsAccountBalanceView {

    @Id
    @Column(name = "account_id", nullable = false)
    private UUID accountId;

    @Column(name = "owner_name", nullable = false, length = 128)
    private String ownerName;

    @Column(name = "balance", nullable = false, precision = 18, scale = 2)
    private BigDecimal balance;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 32)
    private AccountStatus status;

    @Column(name = "last_event_sequence", nullable = false)
    private long lastEventSequence;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;
}
