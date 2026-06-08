package com.kafkalearn.producer;

import com.kafkalearn.config.KafkaTopic;
import com.kafkalearn.event.DeviceStatusChangedEvent;
import com.kafkalearn.exception.EventPublishException;
import com.kafkalearn.messaging.DeviceStatusChangedEventCodec;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaEventPublisher implements EventPublisher{

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final DeviceStatusChangedEventCodec eventCodec;

    @Override
    public CompletableFuture<SendResult<String, String>> publishAsync(DeviceStatusChangedEvent event) {
        return send(KafkaTopic.statusUpdateTopic, event, Boolean.FALSE);
    }

    @Override
    public SendResult<String, String> publishSync(DeviceStatusChangedEvent event) {
        try {
            return send(KafkaTopic.statusUpdateTopic, event, true).get(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new EventPublishException("内部冲突", e);
        } catch (ExecutionException | TimeoutException e) {
            throw new EventPublishException("同步发送事件失败", e);
        }
    }

    /** 唯一发送核心 —— DRY */
    private CompletableFuture<SendResult<String, String>> send(String topic, DeviceStatusChangedEvent event, boolean sync) {
        // 使用deviceId做key，用于保证单设备有序
        String key = event.deviceId().toString();
        String payload = eventCodec.serialize(event);
        log.info("发布事件, topic=[{}], key=[{}], eventId=[{}], status=[{}]", topic, key, event.eventId(), event.active());

        CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send(topic, key, payload);

        // 统一日志回调，替代 CallbackMsg
        future.whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("事件发布失败, eventId=[{}], error=[{}]", event.eventId(), ex.getMessage());
            } else {
                var meta = result.getRecordMetadata();
                log.info("事件发布成功, eventId=[{}], partition=[{}], offset=[{}]", event.eventId(), meta.partition(), meta.offset());
            }
        });
        return future;
    }
}
