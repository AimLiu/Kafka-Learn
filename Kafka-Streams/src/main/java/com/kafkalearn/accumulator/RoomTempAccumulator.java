package com.kafkalearn.accumulator;

import com.kafkalearn.message.TemperatureReading;
import lombok.Getter;

/**
 * 房间温度窗口累加器，供 Kafka Streams {@code aggregate} 使用。
 */
@Getter
public class RoomTempAccumulator {

    private double sum;
    private int count;
    private double max;
    private String homeId;

    /**
     * 创建空累加器。
     *
     * @return 初始状态累加器
     */
    public static RoomTempAccumulator empty() {
        return new RoomTempAccumulator();
    }

    /**
     * 累加一条温湿度读数。
     *
     * @param reading 遥测读数
     * @return 当前累加器实例（便于链式 aggregate）
     */
    public RoomTempAccumulator add(TemperatureReading reading) {
        double temperature = reading.temperatureCelsius();
        sum += temperature;
        count++;
        max = count == 1 ? temperature : Math.max(max, temperature);
        if (homeId == null) {
            homeId = reading.homeId();
        }
        return this;
    }

    /**
     * 计算当前窗口平均温度。
     *
     * @return 均温；无样本时返回 0
     */
    public double avg() {
        return count == 0 ? 0.0 : sum / count;
    }
}
