package com.kafkalearn.message;

/**
 * 房间温度窗口聚合输出消息。
 *
 * @param roomId          房间 ID
 * @param homeId          家庭 ID
 * @param windowStart     窗口起始时间（epoch ms）
 * @param windowEnd       窗口结束时间（epoch ms）
 * @param avgTemperature  窗口内平均温度
 * @param maxTemperature  窗口内最高温度
 * @param sampleCount     样本数量
 */
public record RoomTempStatsRecord(
        String roomId,
        String homeId,
        long windowStart,
        long windowEnd,
        double avgTemperature,
        double maxTemperature,
        int sampleCount
) {
}
