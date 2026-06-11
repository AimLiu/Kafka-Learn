package com.kafkalearn.controller;

import com.kafkalearn.event.DeviceAlertTriggeredEvent;
import com.kafkalearn.producer.EventPublisher;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller("api")
@RequiredArgsConstructor
public class AlertTriggerController {
    final EventPublisher eventPublisher;

    @PostMapping("/alerts/trigger")
    @ResponseBody
    public ResponseEntity triggerAlert(@RequestBody DeviceAlertTriggeredEvent event) {
        //todo: 完善手动触发路径
        SendResult<String, String> result = eventPublisher.publishSync(event);
        return ResponseEntity.ok().body(result.getRecordMetadata());
    }
}
