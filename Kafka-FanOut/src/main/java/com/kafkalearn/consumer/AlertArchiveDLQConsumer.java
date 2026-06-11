package com.kafkalearn.consumer;

import com.kafkalearn.config.KafkaTopic;
import com.kafkalearn.entity.AlertArchiveDLQEntity;
import com.kafkalearn.entity.AlertPushLogEntity;
import com.kafkalearn.event.DeviceAlertTriggeredEvent;
import com.kafkalearn.event.common.Channel;
import com.kafkalearn.event.common.Severity;
import com.kafkalearn.event.common.SimulateMode;
import com.kafkalearn.exception.AlertProcessException;
import com.kafkalearn.messaging.DeviceAlertTriggeredEventCodec;
import com.kafkalearn.repository.AlertArchiveDLQRepository;
import com.kafkalearn.repository.AlertPushLogRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.UUID;

@Service
@Slf4j
@RequiredArgsConstructor
public class AlertArchiveDLQConsumer {

    private final AlertArchiveDLQRepository repository;
    private final DeviceAlertTriggeredEventCodec eventCodec;

    @KafkaListener(
            groupId = "alert-dlq-archive-group",
            topics= KafkaTopic.DEVICE_ALERT_PUSH_DLQ_TOPIC
    )
    @Transactional
    public void archivePushDlq(String payload,
                               @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                               @Header(KafkaHeaders.DLT_EXCEPTION_MESSAGE) String error) {
        log.info("死信队列：消费事件：topic=[{}], error=[{}]", topic, error);
        if (error == null || topic == null) {
            return;
        }
        DeviceAlertTriggeredEvent event = eventCodec.deserialize(payload);
        // 保存到本地，模拟推送
        AlertArchiveDLQEntity dlqEntity = repository.findByEventId(UUID.fromString(event.eventId()));
        if(dlqEntity == null){
            dlqEntity = new AlertArchiveDLQEntity();
            dlqEntity.setId(UUID.randomUUID());
            dlqEntity.setEventId(UUID.fromString(event.eventId()));
            dlqEntity.setSourceTopic(topic);
            dlqEntity.setTargetConsumer("AlertPushConsumer");
            dlqEntity.setRawPayload(payload);
            dlqEntity.setArchivedAt(Timestamp.valueOf(LocalDateTime.now()));
            dlqEntity.setErrorMessage(error);
            repository.save(dlqEntity);
            log.info("死信队列：推送日志保存成功：eventId=[{}], deviceId=[{}]",  event.eventId(), event.deviceId());
        } else {
            //幂等性，不进行重复消费
            return;
        }
    }


    @KafkaListener(
            groupId = "alert-dlq-archive-group",
            topics= KafkaTopic.DEVICE_ALERT_RULE_DLQ_TOPIC
    )
    @Transactional
    public void archiveRuleEngineDlq(String payload,
                               @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                               @Header(KafkaHeaders.DLT_EXCEPTION_MESSAGE) String error) {
        log.info("死信队列：消费事件：topic=[{}], error=[{}]", topic, error);
        if (error == null || topic == null) {
            return;
        }
        DeviceAlertTriggeredEvent event = eventCodec.deserialize(payload);
        // 保存到本地，模拟推送
        AlertArchiveDLQEntity dlqEntity = repository.findByEventId(UUID.fromString(event.eventId()));
        if(dlqEntity == null){
            dlqEntity = new AlertArchiveDLQEntity();
            dlqEntity.setId(UUID.randomUUID());
            dlqEntity.setEventId(UUID.fromString(event.eventId()));
            dlqEntity.setSourceTopic(topic);
            dlqEntity.setTargetConsumer("AlertRuleEngineConsumer");
            dlqEntity.setRawPayload(payload);
            dlqEntity.setArchivedAt(Timestamp.valueOf(LocalDateTime.now()));
            dlqEntity.setErrorMessage(error);
            repository.save(dlqEntity);
            log.info("死信队列：规则日志保存成功：eventId=[{}], deviceId=[{}]",  event.eventId(), event.deviceId());
        } else {
            //幂等性，不进行重复消费
            return;
        }
    }


    @KafkaListener(
            groupId = "alert-dlq-archive-group",
            topics= KafkaTopic.DEVICE_ALERT_STORAGE_DLQ_TOPIC
    )
    @Transactional
    public void archiveStorageDlq(String payload,
                                     @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                                     @Header(KafkaHeaders.DLT_EXCEPTION_MESSAGE) String error) {
        log.info("死信队列：消费事件：topic=[{}], error=[{}]", topic, error);
        if (error == null || topic == null) {
            return;
        }
        DeviceAlertTriggeredEvent event = eventCodec.deserialize(payload);
        // 保存到本地，模拟推送
        AlertArchiveDLQEntity dlqEntity = repository.findByEventId(UUID.fromString(event.eventId()));
        if(dlqEntity == null){
            dlqEntity = new AlertArchiveDLQEntity();
            dlqEntity.setId(UUID.randomUUID());
            dlqEntity.setEventId(UUID.fromString(event.eventId()));
            dlqEntity.setSourceTopic(topic);
            dlqEntity.setTargetConsumer("AlertStorageConsumer");
            dlqEntity.setRawPayload(payload);
            dlqEntity.setArchivedAt(Timestamp.valueOf(LocalDateTime.now()));
            // 从未 setErrorMessage，但 Entity 上 error_message nullable=false
            dlqEntity.setErrorMessage(error);
            repository.save(dlqEntity);
            log.info("死信队列：存储日志保存成功：eventId=[{}], deviceId=[{}]",  event.eventId(), event.deviceId());
        } else {
            //幂等性，不进行重复消费
            return;
        }
    }
}
