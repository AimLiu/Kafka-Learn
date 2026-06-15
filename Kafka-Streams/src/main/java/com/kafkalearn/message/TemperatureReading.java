package com.kafkalearn.message;

/**
 * 温湿度遥测输入消息。
 *
 * @param deviceId            设备 ID
 * @param roomId              房间 ID（聚合维度）
 * @param homeId              家庭 ID
 * @param temperatureCelsius  摄氏温度
 * @param humidityPercent     湿度（可选，不参与计算）
 * @param reportedAt          上报时间戳（epoch ms）
 */
public record TemperatureReading(
        String deviceId,
        String roomId,
        String homeId,
        Double temperatureCelsius,
        Double humidityPercent,
        long reportedAt
) {
}
