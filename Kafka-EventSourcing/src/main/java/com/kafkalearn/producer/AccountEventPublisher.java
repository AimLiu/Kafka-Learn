package com.kafkalearn.producer;

import com.kafkalearn.config.KafkaTopic;
import com.kafkalearn.messaging.AccountDomainEventMessage;
import com.kafkalearn.messaging.AccountEventCodec;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * 账户领域事件 Kafka 发布器。
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class AccountEventPublisher {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final AccountEventCodec eventCodec;

    /**
     * 发布账户领域事件到 Kafka。
     *
     * @param message 事件消息
     */
    public void publish(AccountDomainEventMessage message) {
        String topic = KafkaTopic.ACCOUNT_EVENTS;
        String key = message.aggregateId().toString();
        String payload = eventCodec.serialize(message);
        kafkaTemplate.send(topic, key, payload);
        log.info("账户事件已发布, topic=[{}], eventId=[{}], type=[{}], sequence=[{}]",
                topic, message.eventId(), message.eventType(), message.sequence());
    }
}
