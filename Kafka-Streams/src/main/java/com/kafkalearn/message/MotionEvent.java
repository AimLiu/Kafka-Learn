package com.kafkalearn.message;

/**
 * 人体感应事件输入消息。
 *
 * @param deviceId   人体感应设备 ID
 * @param homeId     家庭 ID（Join Key）
 * @param roomId     房间 ID
 * @param detected   是否检测到移动
 * @param occurredAt 事件时间戳（epoch ms）
 */
public record MotionEvent(
        String deviceId,
        String homeId,
        String roomId,
        boolean detected,
        long occurredAt
) {
}
