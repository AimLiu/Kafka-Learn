package com.kafkalearn.entity;

import com.kafkalearn.event.common.AlertType;
import com.kafkalearn.event.common.Severity;
import com.kafkalearn.event.common.SimulateMode;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.sql.Timestamp;
import java.util.UUID;

@Data
@NoArgsConstructor
@Entity
@Table(name = "alert_storage_log")
public class AlertEventEntity {

    @Id
    @Column(name = "id", nullable = false)
    private UUID id;

    @Column(name = "event_id", nullable = false)
    private UUID eventId;

    @Column(name = "device_id", nullable = false)
    private UUID deviceId;

    @Enumerated(EnumType.STRING)
    @Column(name = "alert_type", nullable = false)
    private AlertType alertType;

    @Enumerated(EnumType.STRING)
    @Column(name = "severity", nullable = false)
    private Severity severity;

    @Column(name = "metric_value")  // 去掉 nullable = false
    private Double metricValue;

    @Column(name = "source", nullable = false)
    private String source;

    // （DLQ 实验）
    @Enumerated(EnumType.STRING)
    @Column(name = "simulate_mode", nullable = false)
    private SimulateMode simulateMode;

    @Column(name = "occured_time", nullable = false)
    private Timestamp occuredTime;

    @Column(name="stored_at", nullable = false)
    private Timestamp storedAt;
}
