package com.kafkalearn.message;

/**
 * 入侵疑似告警输出消息。
 *
 * @param homeId            家庭 ID
 * @param doorDeviceId      门磁设备 ID
 * @param motionDeviceId    人体感应设备 ID
 * @param doorOpenedAt      门开时间
 * @param motionDetectedAt  人体检测时间
 * @param alertType         告警类型
 * @param triggeredAt       触发时间
 */
public record IntrusionAlert(
        String homeId,
        String doorDeviceId,
        String motionDeviceId,
        long doorOpenedAt,
        long motionDetectedAt,
        AlertType alertType,
        long triggeredAt
) {

    /**
     * 由门磁与人体事件构造入侵告警。
     *
     * @param door   门开事件
     * @param motion 人体移动事件
     * @return 入侵告警记录
     */
    public static IntrusionAlert from(DoorEvent door, MotionEvent motion) {
        long triggeredAt = Math.max(door.occurredAt(), motion.occurredAt());
        return new IntrusionAlert(
                door.homeId(),
                door.deviceId(),
                motion.deviceId(),
                door.occurredAt(),
                motion.occurredAt(),
                AlertType.INTRUSION_SUSPECTED,
                triggeredAt
        );
    }
}
