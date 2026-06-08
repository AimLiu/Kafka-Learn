package com.kafkalearn.messaging;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.kafkalearn.entity.EsEvent;
import com.kafkalearn.exception.EventPublishException;

import java.time.Instant;
import java.util.UUID;

/**
 * Kafka 传输用的账户领域事件消息。
 *
 * @param eventId       事件 ID
 * @param aggregateId   聚合 ID
 * @param aggregateType 聚合类型
 * @param eventType     事件类型
 * @param sequence      聚合内序号
 * @param commandId     命令 ID
 * @param payload       事件体
 * @param occurredAt    发生时间
 */
public record AccountDomainEventMessage(
        UUID eventId,
        UUID aggregateId,
        String aggregateType,
        String eventType,
        long sequence,
        UUID commandId,
        JsonNode payload,
        Instant occurredAt
) {

    /**
     * 从持久化事件实体构建消息。
     *
     * @param event 事件实体
     * @return 消息对象
     */
    public static AccountDomainEventMessage from(EsEvent event) {
        return new AccountDomainEventMessage(
                event.getEventId(),
                event.getAggregateId(),
                event.getAggregateType(),
                event.getEventType(),
                event.getSequence(),
                event.getCommandId(),
                event.getPayload(),
                event.getOccurredAt()
        );
    }
}
