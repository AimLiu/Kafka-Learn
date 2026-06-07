package com.kafkalearn.demo.entity;

import com.fasterxml.jackson.databind.JsonNode;
import com.kafkalearn.demo.domain.OutboxStatus;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.FetchType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.LocalDateTime;
import java.util.UUID;

@Entity
@Table(name = "callback_outbox")
public class CallbackOutboxEntity {

    @Id
    private UUID id;

    @ManyToOne(fetch = FetchType.LAZY, optional = false)
    @JoinColumn(name = "task_id", nullable = false)
    private CallbackTaskEntity task;

    @Column(name = "topic_name", nullable = false, length = 128)
    private String topicName;

    @Column(name = "message_key", nullable = false, length = 128)
    private String messageKey;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "message_payload", nullable = false, columnDefinition = "jsonb")
    private JsonNode messagePayload;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 32)
    private OutboxStatus status;

    @Column(name = "publish_retry_count", nullable = false)
    private int publishRetryCount;

    @Column(name = "next_attempt_time", nullable = false)
    private LocalDateTime nextAttemptTime;

    @Column(name = "last_error_msg", length = 255)
    private String lastErrorMsg;

    @Column(name = "created_at", nullable = false)
    private LocalDateTime createdAt;

    @Column(name = "updated_at", nullable = false)
    private LocalDateTime updatedAt;

    public UUID getId() {
        return id;
    }

    public void setId(UUID id) {
        this.id = id;
    }

    public CallbackTaskEntity getTask() {
        return task;
    }

    public void setTask(CallbackTaskEntity task) {
        this.task = task;
    }

    public String getTopicName() {
        return topicName;
    }

    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }

    public String getMessageKey() {
        return messageKey;
    }

    public void setMessageKey(String messageKey) {
        this.messageKey = messageKey;
    }

    public JsonNode getMessagePayload() {
        return messagePayload;
    }

    public void setMessagePayload(JsonNode messagePayload) {
        this.messagePayload = messagePayload;
    }

    public OutboxStatus getStatus() {
        return status;
    }

    public void setStatus(OutboxStatus status) {
        this.status = status;
    }

    public int getPublishRetryCount() {
        return publishRetryCount;
    }

    public void setPublishRetryCount(int publishRetryCount) {
        this.publishRetryCount = publishRetryCount;
    }

    public LocalDateTime getNextAttemptTime() {
        return nextAttemptTime;
    }

    public void setNextAttemptTime(LocalDateTime nextAttemptTime) {
        this.nextAttemptTime = nextAttemptTime;
    }

    public String getLastErrorMsg() {
        return lastErrorMsg;
    }

    public void setLastErrorMsg(String lastErrorMsg) {
        this.lastErrorMsg = lastErrorMsg;
    }

    public LocalDateTime getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(LocalDateTime createdAt) {
        this.createdAt = createdAt;
    }

    public LocalDateTime getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(LocalDateTime updatedAt) {
        this.updatedAt = updatedAt;
    }
}
