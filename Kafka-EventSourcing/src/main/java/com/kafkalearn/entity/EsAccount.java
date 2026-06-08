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

import java.time.Instant;
import java.util.UUID;

/**
 * 账户聚合根元数据实体，对应表 {@code es_account}。
 */
@Getter
@Setter
@NoArgsConstructor
@Entity
@Table(name = "es_account")
public class EsAccount {

    @Id
    @Column(name = "account_id", nullable = false)
    private UUID accountId;

    @Column(name = "owner_name", nullable = false, length = 128)
    private String ownerName;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 32)
    private AccountStatus status;

    @Column(name = "current_version", nullable = false)
    private long currentVersion;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;
}
