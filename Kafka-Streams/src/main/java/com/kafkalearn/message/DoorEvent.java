package com.kafkalearn.message;

/**
 * 门磁事件输入消息。
 *
 * @param deviceId   门磁设备 ID
 * @param homeId     家庭 ID（Join Key）
 * @param state      OPEN 或 CLOSED
 * @param occurredAt 事件时间戳（epoch ms）
 */
public record DoorEvent(
        String deviceId,
        String homeId,
        String state,
        long occurredAt
) {

    /** 门开状态常量。 */
    public static final String STATE_OPEN = "OPEN";
}
