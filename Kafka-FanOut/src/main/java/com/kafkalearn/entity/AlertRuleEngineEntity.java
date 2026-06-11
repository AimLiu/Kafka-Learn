package com.kafkalearn.entity;

import com.kafkalearn.event.common.Channel;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.kafka.common.protocol.types.Field;

import java.sql.Timestamp;
import java.util.UUID;

@Data
@NoArgsConstructor
@Entity
@Table(name = "alert_rule_hit_log")
public class AlertRuleEngineEntity {

    @Id
    @Column(name = "id", nullable = false)
    private UUID id;

    @Column(name = "event_id", nullable = false)
    private UUID eventId;

    @Column(name = "device_id", nullable = false)
    private UUID deviceId;

    @Column(name = "rule_name", nullable = false)
    private String ruleName;

    @Column(name = "action", nullable = false)
    private String action;

    @Column(name="processed_at", nullable = false)
    private Timestamp processedAt;
}
