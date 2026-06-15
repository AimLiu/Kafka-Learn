package com.kafkalearn.consumer;

import com.kafkalearn.config.KafkaTopic;
import com.kafkalearn.message.IntrusionAlert;
import com.kafkalearn.message.RoomTempStatsRecord;
import com.kafkalearn.message.TempThresholdAlert;
import com.kafkalearn.messaging.EventJsonCodec;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * 订阅 Streams 输出 Topic，以统一前缀打印日志，便于 L1–L6 本地验收。
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class StreamsOutputLogConsumer {

    private final EventJsonCodec eventJsonCodec;

    /**
     * 消费房间温度窗口统计。
     *
     * @param record Kafka 消费记录
     */
    @KafkaListener(
            topics = KafkaTopic.STREAM_ROOM_TEMP_STATS,
            groupId = KafkaTopic.STREAMS_OUTPUT_LOG_GROUP
    )
    public void onRoomTempStats(ConsumerRecord<String, String> record) {
        RoomTempStatsRecord stats = eventJsonCodec.deserialize(record.value(), RoomTempStatsRecord.class);
        if (stats == null) {
            return;
        }
        log.info("[STATS] room={} avg={} max={} count={} window=[{}-{}]",
                stats.roomId(),
                stats.avgTemperature(),
                stats.maxTemperature(),
                stats.sampleCount(),
                stats.windowStart(),
                stats.windowEnd());
    }

    /**
     * 消费超温告警。
     *
     * @param record Kafka 消费记录
     */
    @KafkaListener(
            topics = KafkaTopic.STREAM_TEMP_THRESHOLD_ALERT,
            groupId = KafkaTopic.STREAMS_OUTPUT_LOG_GROUP
    )
    public void onTempThresholdAlert(ConsumerRecord<String, String> record) {
        TempThresholdAlert alert = eventJsonCodec.deserialize(record.value(), TempThresholdAlert.class);
        if (alert == null) {
            return;
        }
        log.info("[TEMP-ALERT] room={} avg={} threshold={} window=[{}-{}]",
                alert.roomId(),
                alert.avgTemperature(),
                alert.threshold(),
                alert.windowStart(),
                alert.windowEnd());
    }

    /**
     * 消费入侵告警。
     *
     * @param record Kafka 消费记录
     */
    @KafkaListener(
            topics = KafkaTopic.STREAM_INTRUSION_ALERT,
            groupId = KafkaTopic.STREAMS_OUTPUT_LOG_GROUP
    )
    public void onIntrusionAlert(ConsumerRecord<String, String> record) {
        IntrusionAlert alert = eventJsonCodec.deserialize(record.value(), IntrusionAlert.class);
        if (alert == null) {
            return;
        }
        log.info("[INTRUSION] home={} door@{} motion@{} type={}",
                alert.homeId(),
                alert.doorOpenedAt(),
                alert.motionDetectedAt(),
                alert.alertType());
    }
}
