package com.kafkalearn.topology;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafkalearn.config.KafkaTopic;
import com.kafkalearn.config.StreamsAppProperties;
import com.kafkalearn.domain.SmartHomeConstants;
import com.kafkalearn.message.RoomTempStatsRecord;
import com.kafkalearn.message.TempThresholdAlert;
import com.kafkalearn.message.TemperatureReading;
import com.kafkalearn.messaging.EventJsonCodec;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link RoomTemperatureTopology} TopologyTestDriver 集成测试。
 */
class RoomTemperatureTopologyTest {

    private TopologyTestDriver testDriver;
    private EventJsonCodec eventJsonCodec;
    private TestInputTopic<String, String> inputTopic;
    private TestOutputTopic<String, String> statsTopic;
    private TestOutputTopic<String, String> alertTopic;

    @BeforeEach
    void setUp() {
        eventJsonCodec = new EventJsonCodec(new ObjectMapper());
        StreamsAppProperties properties = new StreamsAppProperties();
        properties.setTempThresholdCelsius(30.0);
        properties.setTempWindowMinutes(1);

        StreamsBuilder builder = new StreamsBuilder();
        new RoomTemperatureTopology(properties, eventJsonCodec).build(builder);

        Properties config = new Properties();
        config.put(APPLICATION_ID_CONFIG, "room-temp-topology-test");
        config.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        config.put(DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);

        testDriver = new TopologyTestDriver(builder.build(), config);
        inputTopic = testDriver.createInputTopic(
                KafkaTopic.DEVICE_TELEMETRY_TEMPERATURE,
                Serdes.String().serializer(),
                Serdes.String().serializer());
        statsTopic = testDriver.createOutputTopic(
                KafkaTopic.STREAM_ROOM_TEMP_STATS,
                Serdes.String().deserializer(),
                Serdes.String().deserializer());
        alertTopic = testDriver.createOutputTopic(
                KafkaTopic.STREAM_TEMP_THRESHOLD_ALERT,
                Serdes.String().deserializer(),
                Serdes.String().deserializer());
    }

    @AfterEach
    void tearDown() {
        testDriver.close();
    }

    @Test
    void shouldAggregateThreeReadingsInOneWindow() {
        long baseTime = 1_000L;
        pipeReading(22.0, baseTime);
        pipeReading(24.0, baseTime + 10_000L);
        pipeReading(26.0, baseTime + 20_000L);

        testDriver.advanceWallClockTime(Duration.ofMinutes(1));

        List<TestRecord<String, String>> statsRecords = statsTopic.readRecordsToList();
        assertFalse(statsRecords.isEmpty());

        RoomTempStatsRecord stats = eventJsonCodec.deserialize(
                statsRecords.getLast().value(), RoomTempStatsRecord.class);
        assertEquals(3, stats.sampleCount());
        assertEquals(24.0, stats.avgTemperature(), 0.001);
        assertEquals(SmartHomeConstants.ROOM_KITCHEN, stats.roomId());
    }

    @Test
    void shouldEmitTempAlertWhenAverageExceedsThreshold() {
        long baseTime = 2_000L;
        pipeReading(32.0, baseTime);
        pipeReading(34.0, baseTime + 5_000L);

        testDriver.advanceWallClockTime(Duration.ofMinutes(1));

        assertFalse(alertTopic.isEmpty());
        TempThresholdAlert alert = eventJsonCodec.deserialize(
                alertTopic.readRecord().value(), TempThresholdAlert.class);
        assertTrue(alert.avgTemperature() > 30.0);
        assertEquals(SmartHomeConstants.ROOM_KITCHEN, alert.roomId());
    }

    private void pipeReading(double temperature, long timestamp) {
        TemperatureReading reading = new TemperatureReading(
                SmartHomeConstants.SENSOR_KITCHEN,
                SmartHomeConstants.ROOM_KITCHEN,
                SmartHomeConstants.HOME_ID,
                temperature,
                45.0,
                timestamp
        );
        inputTopic.pipeInput(
                SmartHomeConstants.SENSOR_KITCHEN,
                eventJsonCodec.serialize(reading),
                timestamp
        );
    }
}
