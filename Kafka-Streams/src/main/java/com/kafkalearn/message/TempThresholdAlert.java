package com.kafkalearn.message;

/**
 * 超温告警输出消息。
 *
 * @param roomId          房间 ID
 * @param homeId          家庭 ID
 * @param avgTemperature  触发告警的窗口均温
 * @param threshold       阈值
 * @param windowStart     窗口起始时间
 * @param windowEnd       窗口结束时间
 * @param alertType       告警类型
 * @param triggeredAt     触发时间
 */
public record TempThresholdAlert(
        String roomId,
        String homeId,
        double avgTemperature,
        double threshold,
        long windowStart,
        long windowEnd,
        AlertType alertType,
        long triggeredAt
) {
}
