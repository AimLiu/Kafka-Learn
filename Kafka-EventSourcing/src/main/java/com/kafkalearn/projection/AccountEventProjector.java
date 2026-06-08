package com.kafkalearn.projection;

import com.kafkalearn.messaging.AccountDomainEventMessage;
import com.kafkalearn.messaging.AccountEventCodec;
import com.kafkalearn.service.AccountProjectionService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * 账户领域事件 Kafka 投影消费者。
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class AccountEventProjector {

    private final AccountEventCodec eventCodec;
    private final AccountProjectionService projectionService;

    /**
     * 消费账户领域事件并更新读模型。
     *
     * @param message JSON 事件载荷
     */
    @KafkaListener(
            topics = "${app.kafka.account-events-topic}",
            groupId = "${spring.kafka.consumer.group-id}"
    )
    public void onMessage(String message) {
        AccountDomainEventMessage event = eventCodec.deserialize(message);
        projectionService.project(event);
    }
}
