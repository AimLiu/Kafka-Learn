package com.kafkalearn.consumer;

import com.kafkalearn.config.KafkaTopic;
import com.kafkalearn.entity.AlertEventEntity;
import com.kafkalearn.entity.AlertPushLogEntity;
import com.kafkalearn.event.DeviceAlertTriggeredEvent;
import com.kafkalearn.event.common.AlertType;
import com.kafkalearn.event.common.Channel;
import com.kafkalearn.event.common.Severity;
import com.kafkalearn.event.common.SimulateMode;
import com.kafkalearn.exception.AlertProcessException;
import com.kafkalearn.messaging.DeviceAlertTriggeredEventCodec;
import com.kafkalearn.repository.AlertPushLogRepository;
import com.kafkalearn.repository.DeviceAlertEventRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.UUID;

@Service
@Slf4j
@RequiredArgsConstructor
public class AlertPushConsumer {

    private final AlertPushLogRepository repository;
    private final DeviceAlertTriggeredEventCodec eventCodec;

    @KafkaListener(
            groupId = "alert-push-group",
            topics= KafkaTopic.DEVICE_ALERT_TOPIC,
            containerFactory = "pushKafkaListenerContainerFactory"
    )
    @Transactional
    public void consumer(String message){
        DeviceAlertTriggeredEvent event = eventCodec.deserialize(message);
        log.info("推送组：消费事件：eventId=[{}], deviceId=[{}], simulate=[{}]", event.eventId(), event.deviceId(), event.simulateMode());

        //各 Consumer 在 FAIL_STORAGE / FAIL_PUSH / FAIL_RULE 时抛 AlertProcessException。
        if (event.simulateMode().equals(SimulateMode.FAIL_PUSH)) {
            log.error("推送组：消费事件失败：eventId=[{}], deviceId=[{}]",  event.eventId(), event.deviceId());
            throw new AlertProcessException("模拟推送失败, eventId=" + event.eventId());
        }

        // 保存到本地，模拟推送
        AlertPushLogEntity logEntity = repository.findByEventId(UUID.fromString(event.eventId()));
        if(logEntity == null){
            logEntity = new AlertPushLogEntity();
            logEntity.setId(UUID.randomUUID());
            logEntity.setEventId(UUID.fromString(event.eventId()));
            logEntity.setChannel(event.severity().equals(Severity.CRITICAL)? Channel.SMS: Channel.APP);
            logEntity.setDeviceId(event.deviceId());
            logEntity.setPushStatus("SENT");
            logEntity.setPushedAt(Timestamp.valueOf(LocalDateTime.now()));
            repository.save(logEntity);
            log.info("推送组：推送日志保存成功：eventId=[{}], deviceId=[{}]",  event.eventId(), event.deviceId());
        } else {
            //幂等性，不进行重复消费
            return;
        }
    }
}
