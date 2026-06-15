package com.kafkalearn.accumulator;

import com.kafkalearn.domain.SmartHomeConstants;
import com.kafkalearn.message.TemperatureReading;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * {@link RoomTempAccumulator} 单元测试。
 */
class RoomTempAccumulatorTest {

    @Test
    void emptyAccumulatorShouldHaveZeroCountAndAvg() {
        RoomTempAccumulator accumulator = RoomTempAccumulator.empty();

        assertEquals(0, accumulator.getCount());
        assertEquals(0.0, accumulator.avg());
    }

    @Test
    void singleReadingShouldUpdateAvgAndMax() {
        RoomTempAccumulator accumulator = RoomTempAccumulator.empty()
                .add(reading(25.0));

        assertEquals(1, accumulator.getCount());
        assertEquals(25.0, accumulator.avg());
        assertEquals(25.0, accumulator.getMax());
    }

    @Test
    void multipleReadingsShouldCalculateAvgAndMax() {
        RoomTempAccumulator accumulator = RoomTempAccumulator.empty()
                .add(reading(20.0))
                .add(reading(30.0));

        assertEquals(2, accumulator.getCount());
        assertEquals(25.0, accumulator.avg());
        assertEquals(30.0, accumulator.getMax());
    }

    private TemperatureReading reading(double temperature) {
        return new TemperatureReading(
                SmartHomeConstants.SENSOR_KITCHEN,
                SmartHomeConstants.ROOM_KITCHEN,
                SmartHomeConstants.HOME_ID,
                temperature,
                45.0,
                System.currentTimeMillis()
        );
    }
}
