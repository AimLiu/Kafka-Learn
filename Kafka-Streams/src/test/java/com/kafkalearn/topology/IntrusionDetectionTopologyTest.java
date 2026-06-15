package com.kafkalearn.topology;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafkalearn.config.KafkaTopic;
import com.kafkalearn.config.StreamsAppProperties;
import com.kafkalearn.domain.SmartHomeConstants;
import com.kafkalearn.message.DoorEvent;
import com.kafkalearn.message.IntrusionAlert;
import com.kafkalearn.message.MotionEvent;
import com.kafkalearn.messaging.EventJsonCodec;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link IntrusionDetectionTopology} TopologyTestDriver 集成测试。
 */
class IntrusionDetectionTopologyTest {

    private TopologyTestDriver testDriver;
    private EventJsonCodec eventJsonCodec;
    private TestInputTopic<String, String> doorTopic;
    private TestInputTopic<String, String> motionTopic;
    private TestOutputTopic<String, String> alertTopic;

    @BeforeEach
    void setUp() {
        eventJsonCodec = new EventJsonCodec(new ObjectMapper());
        StreamsAppProperties properties = new StreamsAppProperties();
        properties.setIntrusionJoinSeconds(30);

        StreamsBuilder builder = new StreamsBuilder();
        new IntrusionDetectionTopology(properties, eventJsonCodec).build(builder);

        Properties config = new Properties();
        config.put(APPLICATION_ID_CONFIG, "intrusion-topology-test");
        config.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        config.put(DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);

        testDriver = new TopologyTestDriver(builder.build(), config);
        doorTopic = testDriver.createInputTopic(
                KafkaTopic.DEVICE_EVENT_DOOR,
                Serdes.String().serializer(),
                Serdes.String().serializer());
        motionTopic = testDriver.createInputTopic(
                KafkaTopic.DEVICE_EVENT_MOTION,
                Serdes.String().serializer(),
                Serdes.String().serializer());
        alertTopic = testDriver.createOutputTopic(
                KafkaTopic.STREAM_INTRUSION_ALERT,
                Serdes.String().deserializer(),
                Serdes.String().deserializer());
    }

    @AfterEach
    void tearDown() {
        testDriver.close();
    }

    @Test
    void shouldJoinDoorAndMotionWithinWindow() {
        long doorTime = 10_000L;
        pipeDoor(doorTime);
        pipeMotion(doorTime + 1_000L);

        assertFalse(alertTopic.isEmpty());
        IntrusionAlert alert = eventJsonCodec.deserialize(alertTopic.readRecord().value(), IntrusionAlert.class);
        assertEquals(SmartHomeConstants.HOME_ID, alert.homeId());
    }

    @Test
    void shouldNotEmitAlertWhenOnlyDoorEventProvided() {
        pipeDoor(20_000L);
        assertTrue(alertTopic.isEmpty());
    }

    @Test
    void shouldNotJoinWhenEventsAreTooFarApart() {
        long doorTime = 30_000L;
        pipeDoor(doorTime);
        pipeMotion(doorTime + 35_000L);
        assertTrue(alertTopic.isEmpty());
    }

    private void pipeDoor(long occurredAt) {
        DoorEvent door = new DoorEvent(
                SmartHomeConstants.DOOR_ENTRY,
                SmartHomeConstants.HOME_ID,
                DoorEvent.STATE_OPEN,
                occurredAt
        );
        doorTopic.pipeInput(SmartHomeConstants.HOME_ID, eventJsonCodec.serialize(door), occurredAt);
    }

    private void pipeMotion(long occurredAt) {
        MotionEvent motion = new MotionEvent(
                SmartHomeConstants.MOTION_LIVING,
                SmartHomeConstants.HOME_ID,
                SmartHomeConstants.ROOM_LIVING,
                true,
                occurredAt
        );
        motionTopic.pipeInput(SmartHomeConstants.HOME_ID, eventJsonCodec.serialize(motion), occurredAt);
    }
}
