package com.kafkalearn.message;

/**
 * 告警类型枚举。
 */
public enum AlertType {

    /** 房间均温超过阈值。 */
    TEMP_HIGH,

    /** 门开且短时间内检测到人体移动。 */
    INTRUSION_SUSPECTED
}
