package com.kafkalearn.topology;

import com.kafkalearn.config.KafkaTopic;
import com.kafkalearn.config.StreamsAppProperties;
import com.kafkalearn.message.DoorEvent;
import com.kafkalearn.message.IntrusionAlert;
import com.kafkalearn.message.MotionEvent;
import com.kafkalearn.messaging.EventJsonCodec;
import com.kafkalearn.serde.JsonSerdeFactory;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.springframework.stereotype.Component;

import java.time.Duration;

/**
 * 流水线 2：门磁 OPEN × 人体 detected → 30 秒 Stream-Stream Join → 入侵告警。
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class IntrusionDetectionTopology {

    private final StreamsAppProperties streamsAppProperties;
    private final EventJsonCodec eventJsonCodec;

    /**
     * 将入侵检测 Join Topology 注册到 StreamsBuilder。
     *
     * @param builder StreamsBuilder 实例
     */
    public void build(StreamsBuilder builder) {
        var doorSerde = JsonSerdeFactory.jsonSerde(DoorEvent.class, eventJsonCodec.getObjectMapper());
        var motionSerde = JsonSerdeFactory.jsonSerde(MotionEvent.class, eventJsonCodec.getObjectMapper());

        KStream<String, DoorEvent> doors = builder.stream(
                        KafkaTopic.DEVICE_EVENT_DOOR,
                        Consumed.with(Serdes.String(), Serdes.String()))
                .<DoorEvent>mapValues(json -> eventJsonCodec.deserialize(json, DoorEvent.class))
                .filter((key, door) -> door != null && DoorEvent.STATE_OPEN.equals(door.state()));

        KStream<String, MotionEvent> motions = builder.stream(
                        KafkaTopic.DEVICE_EVENT_MOTION,
                        Consumed.with(Serdes.String(), Serdes.String()))
                .<MotionEvent>mapValues(json -> eventJsonCodec.deserialize(json, MotionEvent.class))
                .filter((key, motion) -> motion != null && motion.detected());

        Duration joinWindow = Duration.ofSeconds(streamsAppProperties.getIntrusionJoinSeconds());

        doors.join(
                        motions,
                        IntrusionAlert::from,
                        JoinWindows.ofTimeDifferenceWithNoGrace(joinWindow),
                        StreamJoined.with(Serdes.String(), doorSerde, motionSerde))
                .mapValues(eventJsonCodec::serialize)
                .peek((homeId, json) -> log.info("[INTRUSION-TOPO] home={} payload={}", homeId, json))
                .to(KafkaTopic.STREAM_INTRUSION_ALERT, Produced.with(Serdes.String(), Serdes.String()));
    }
}
