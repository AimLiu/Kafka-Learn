package com.kafkalearn.consumer;

import com.kafkalearn.config.KafkaTopic;
import com.kafkalearn.entity.DeviceStatus;
import com.kafkalearn.event.DeviceStatusChangedEvent;
import com.kafkalearn.messaging.DeviceStatusChangedEventCodec;
import com.kafkalearn.repository.DeviceStatusRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Timestamp;

@Slf4j
@Service
@RequiredArgsConstructor
public class DeviceStatusEventConsumer {

    private final DeviceStatusRepository repository;
    private final DeviceStatusChangedEventCodec eventCodec;

    @KafkaListener(
            groupId = "${spring.kafka.consumer.group-id}",
            topics= KafkaTopic.statusUpdateTopic
    )
    @Transactional
    public void consumer(String message){
        DeviceStatusChangedEvent event = eventCodec.deserialize(message);
        log.info("消费事件：eventId=[{}], deviceId=[{}], status=[{}]", event.eventId(), event.deviceId(), event.active());
        DeviceStatus status = repository.findByDeviceId(event.deviceId());
        if(status == null){
            log.warn("当前设备id=[{}] 不存在", event.deviceId());
            return;
        }
        status.setActive(event.active());
        status.setLastConnectTime(new Timestamp(event.occurredAt()));
        repository.save(status);
        log.info("设备状态已更新, deviceId=[{}], active=[{}]", event.deviceId(), event.active());
    }
}
