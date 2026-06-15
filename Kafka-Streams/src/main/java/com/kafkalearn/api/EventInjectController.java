package com.kafkalearn.api;

import com.kafkalearn.config.KafkaTopic;
import com.kafkalearn.message.DoorEvent;
import com.kafkalearn.message.MotionEvent;
import com.kafkalearn.message.TemperatureReading;
import com.kafkalearn.producer.InputEventPublisher;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * HTTP 手动注入 IoT 事件，支撑 L3–L6 学习场景验收。
 */
@RestController
@RequestMapping("/api/inject")
@RequiredArgsConstructor
public class EventInjectController {

    private final InputEventPublisher inputEventPublisher;

    /**
     * 注入温湿度遥测事件。
     *
     * @param reading 遥测消息体
     * @return 202 Accepted
     */
    @PostMapping("/temperature")
    public ResponseEntity<Map<String, String>> injectTemperature(@RequestBody TemperatureReading reading) {
        inputEventPublisher.publishAsync(
                KafkaTopic.DEVICE_TELEMETRY_TEMPERATURE,
                reading.deviceId(),
                reading
        );
        return accepted(KafkaTopic.DEVICE_TELEMETRY_TEMPERATURE);
    }

    /**
     * 注入门磁事件。
     *
     * @param door 门磁消息体
     * @return 202 Accepted
     */
    @PostMapping("/door")
    public ResponseEntity<Map<String, String>> injectDoor(@RequestBody DoorEvent door) {
        inputEventPublisher.publishAsync(KafkaTopic.DEVICE_EVENT_DOOR, door.homeId(), door);
        return accepted(KafkaTopic.DEVICE_EVENT_DOOR);
    }

    /**
     * 注入人体感应事件。
     *
     * @param motion 人体消息体
     * @return 202 Accepted
     */
    @PostMapping("/motion")
    public ResponseEntity<Map<String, String>> injectMotion(@RequestBody MotionEvent motion) {
        inputEventPublisher.publishAsync(KafkaTopic.DEVICE_EVENT_MOTION, motion.homeId(), motion);
        return accepted(KafkaTopic.DEVICE_EVENT_MOTION);
    }

    private ResponseEntity<Map<String, String>> accepted(String topic) {
        return ResponseEntity.accepted().body(Map.of(
                "status", "accepted",
                "topic", topic
        ));
    }
}
