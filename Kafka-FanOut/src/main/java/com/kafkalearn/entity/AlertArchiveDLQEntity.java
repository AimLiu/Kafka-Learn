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
import org.apache.kafka.common.protocol.types.Field;

import java.sql.Timestamp;
import java.util.UUID;

@Data
@NoArgsConstructor
@Entity
@Table(name = "alert_dlq_archive")
public class AlertArchiveDLQEntity {

    @Id
    @Column(name = "id", nullable = false)
    private UUID id;

    @Column(name = "event_id", nullable = false)
    private UUID eventId;

    @Column(name = "source_topic", nullable = false)
    private String sourceTopic;

    @Column(name = "target_consumer", nullable = false)
    private String targetConsumer;

    @Column(name = "rawPayload", nullable = false)
    private String rawPayload;

    @Column(name = "error_message", nullable = false)
    private String errorMessage;


    @Column(name="archived_at", nullable = false)
    private Timestamp archivedAt;
}
