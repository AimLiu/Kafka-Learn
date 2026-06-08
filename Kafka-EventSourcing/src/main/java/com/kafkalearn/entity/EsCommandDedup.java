package com.kafkalearn.entity;

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
 * 命令幂等记录实体，对应表 {@code es_command_dedup}。
 */
@Getter
@Setter
@NoArgsConstructor
@Entity
@Table(name = "es_command_dedup")
public class EsCommandDedup {

    @Id
    @Column(name = "command_id", nullable = false)
    private UUID commandId;

    @Column(name = "command_type", nullable = false, length = 64)
    private String commandType;

    @Column(name = "aggregate_id")
    private UUID aggregateId;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 32)
    private CommandStatus status;

    @Column(name = "result_event_id")
    private UUID resultEventId;

    @Column(name = "error_message", length = 512)
    private String errorMessage;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    /**
     * 命令幂等状态枚举。
     */
    public enum CommandStatus {
        PROCESSING,
        SUCCEEDED,
        FAILED
    }
}
