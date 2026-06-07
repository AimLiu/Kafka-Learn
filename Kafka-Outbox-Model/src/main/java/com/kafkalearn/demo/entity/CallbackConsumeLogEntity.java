package com.kafkalearn.demo.entity;

import com.kafkalearn.demo.domain.ConsumeResult;
import com.kafkalearn.demo.domain.SimulateMode;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.FetchType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;

import java.time.LocalDateTime;
import java.util.UUID;

@Entity
@Table(name = "callback_consume_log")
public class CallbackConsumeLogEntity {

    @Id
    private UUID id;

    @ManyToOne(fetch = FetchType.LAZY, optional = false)
    @JoinColumn(name = "task_id", nullable = false)
    private CallbackTaskEntity task;

    @Column(name = "consumer_group", nullable = false, length = 128)
    private String consumerGroup;

    @Column(name = "topic_name", nullable = false, length = 128)
    private String topicName;

    @Column(name = "partition_no", nullable = false)
    private int partitionNo;

    @Column(name = "offset_no", nullable = false)
    private long offsetNo;

    @Enumerated(EnumType.STRING)
    @Column(name = "simulate_mode", nullable = false, length = 16)
    private SimulateMode simulateMode;

    @Enumerated(EnumType.STRING)
    @Column(name = "consume_result", nullable = false, length = 32)
    private ConsumeResult consumeResult;

    @Column(name = "error_msg", length = 255)
    private String errorMsg;

    @Column(name = "created_at", nullable = false)
    private LocalDateTime createdAt;

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

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    public String getTopicName() {
        return topicName;
    }

    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }

    public int getPartitionNo() {
        return partitionNo;
    }

    public void setPartitionNo(int partitionNo) {
        this.partitionNo = partitionNo;
    }

    public long getOffsetNo() {
        return offsetNo;
    }

    public void setOffsetNo(long offsetNo) {
        this.offsetNo = offsetNo;
    }

    public SimulateMode getSimulateMode() {
        return simulateMode;
    }

    public void setSimulateMode(SimulateMode simulateMode) {
        this.simulateMode = simulateMode;
    }

    public ConsumeResult getConsumeResult() {
        return consumeResult;
    }

    public void setConsumeResult(ConsumeResult consumeResult) {
        this.consumeResult = consumeResult;
    }

    public String getErrorMsg() {
        return errorMsg;
    }

    public void setErrorMsg(String errorMsg) {
        this.errorMsg = errorMsg;
    }

    public LocalDateTime getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(LocalDateTime createdAt) {
        this.createdAt = createdAt;
    }
}
