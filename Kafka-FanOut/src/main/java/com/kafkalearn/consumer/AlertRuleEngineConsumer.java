package com.kafkalearn.consumer;

import com.kafkalearn.config.KafkaTopic;
import com.kafkalearn.entity.AlertPushLogEntity;
import com.kafkalearn.entity.AlertRuleEngineEntity;
import com.kafkalearn.event.DeviceAlertTriggeredEvent;
import com.kafkalearn.event.common.AlertType;
import com.kafkalearn.event.common.Channel;
import com.kafkalearn.event.common.Severity;
import com.kafkalearn.event.common.SimulateMode;
import com.kafkalearn.exception.AlertProcessException;
import com.kafkalearn.messaging.DeviceAlertTriggeredEventCodec;
import com.kafkalearn.repository.AlertPushLogRepository;
import com.kafkalearn.repository.AlertRuleEngineRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.UUID;

@Service
@Slf4j
@RequiredArgsConstructor
public class AlertRuleEngineConsumer {

    private final AlertRuleEngineRepository repository;
    private final DeviceAlertTriggeredEventCodec eventCodec;

    @KafkaListener(
            groupId = "alert-rule-group",
            topics= KafkaTopic.DEVICE_ALERT_TOPIC,
            containerFactory = "ruleEngineKafkaListenerContainerFactory"
    )
    @Transactional
    public void consumer(String message){
        DeviceAlertTriggeredEvent event = eventCodec.deserialize(message);
        log.info("规则组：消费事件：eventId=[{}], deviceId=[{}], simulate=[{}]", event.eventId(), event.deviceId(), event.simulateMode());

        //各 Consumer 在 FAIL_STORAGE / FAIL_PUSH / FAIL_RULE 时抛 AlertProcessException。
        if (event.simulateMode().equals(SimulateMode.FAIL_RULE)) {
            log.error("规则组：消费事件失败：eventId=[{}], deviceId=[{}]",  event.eventId(), event.deviceId());
            throw new AlertProcessException("模拟规则引擎失败, eventId=" + event.eventId());
        }

        // 保存到本地，模拟推送
        AlertRuleEngineEntity ruleEngineEntity = repository.findByEventId(UUID.fromString(event.eventId()));
        if(ruleEngineEntity == null){
            ruleEngineEntity =  new AlertRuleEngineEntity();
            if(event.severity().equals(Severity.CRITICAL)){
                ruleEngineEntity.setRuleName("CRITICAL_ESCALATE");
                ruleEngineEntity.setAction("ESCALATE");
            } else if((event.alertType().equals(AlertType.TEMP_HIGH) && event.metricValue() > 80)) {
                ruleEngineEntity.setRuleName("TEMP_THRESHOLD");
                ruleEngineEntity.setAction("OPEN_TICKET");
            }else {
                return;
            }
            ruleEngineEntity.setId(UUID.randomUUID());
            ruleEngineEntity.setEventId(UUID.fromString(event.eventId()));
            ruleEngineEntity.setDeviceId(event.deviceId());
            ruleEngineEntity.setProcessedAt(Timestamp.valueOf(LocalDateTime.now()));
            repository.save(ruleEngineEntity);
            log.info("规则组：规则记录保存成功：eventId=[{}], deviceId=[{}]",  event.eventId(), event.deviceId());
        } else {
            //幂等性，不进行重复消费
            return;
        }
    }
}
