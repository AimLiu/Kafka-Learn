package com.kafkalearn.entity;

import com.kafkalearn.event.common.AlertType;
import com.kafkalearn.event.common.Channel;
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
@Table(name = "alert_push_log")
public class AlertPushLogEntity {

    @Id
    @Column(name = "id", nullable = false)
    private UUID id;

    @Column(name = "event_id", nullable = false)
    private UUID eventId;

    @Column(name = "device_id", nullable = false)
    private UUID deviceId;

    @Enumerated(EnumType.STRING)
    @Column(name = "channel", nullable = false)
    private Channel channel;

    @Column(name = "push_status", nullable = false)
    private String pushStatus;

    @Column(name="pushed_at", nullable = false)
    private Timestamp pushedAt;
}
