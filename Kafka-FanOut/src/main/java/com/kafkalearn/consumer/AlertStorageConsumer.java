package com.kafkalearn.consumer;

import com.kafkalearn.config.KafkaTopic;
import com.kafkalearn.entity.AlertEventEntity;
import com.kafkalearn.event.DeviceAlertTriggeredEvent;
import com.kafkalearn.event.common.AlertType;
import com.kafkalearn.event.common.Severity;
import com.kafkalearn.event.common.SimulateMode;
import com.kafkalearn.exception.AlertProcessException;
import com.kafkalearn.messaging.DeviceAlertTriggeredEventCodec;
import com.kafkalearn.repository.DeviceAlertEventRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class AlertStorageConsumer {

    private final DeviceAlertEventRepository repository;
    private final DeviceAlertTriggeredEventCodec eventCodec;

    @KafkaListener(
            groupId = "alert-storage-group",
            topics= KafkaTopic.DEVICE_ALERT_TOPIC,
            // 指定工厂创建监听器，这个工厂配置了错误处理方式等等（死信队列）
            // containerFactory to use to create the message listener container responsible to serve this endpoint.
            containerFactory  = "storageKafkaListenerContainerFactory"
    )
    @Transactional
    public void consumer(String message){
        DeviceAlertTriggeredEvent event = eventCodec.deserialize(message);
        log.info("存储组：消费事件：eventId=[{}], deviceId=[{}], simulate=[{}]", event.eventId(), event.deviceId(), event.simulateMode());

        //各 Consumer 在 FAIL_STORAGE / FAIL_PUSH / FAIL_RULE 时抛 AlertProcessException。
        if (event.simulateMode().equals(SimulateMode.FAIL_STORAGE)) {
            log.error("存储组：消费事件失败：eventId=[{}], deviceId=[{}]",  event.eventId(), event.deviceId());
            throw new AlertProcessException("模拟存储失败, eventId=" + event.eventId());
        }

        AlertEventEntity isUnique = repository.findByEventId(UUID.fromString(event.eventId()));
        if(isUnique == null){
            //进行保存
            AlertEventEntity entity = new AlertEventEntity();
            entity.setId(UUID.randomUUID());
            entity.setDeviceId(event.deviceId());
            entity.setAlertType(event.alertType());
            entity.setSimulateMode(event.simulateMode());
            entity.setEventId(UUID.fromString(event.eventId()));
            entity.setSource(event.source());
            entity.setSeverity(event.severity());
            entity.setOccuredTime(new Timestamp(event.occurredAt()));
            // 未 setMetricValue / setStoredAt
            entity.setMetricValue(event.metricValue());
            entity.setStoredAt(Timestamp.valueOf(LocalDateTime.now()));
            repository.save(entity);
            log.info("存储组：设备告警事件已保存：eventId=[{}], deviceId=[{}]", event.eventId(), event.deviceId());
        } else {
            //幂等性，不进行重复消费
            return;
        }
    }
}
