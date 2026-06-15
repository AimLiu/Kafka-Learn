package com.kafkalearn.config;

/**
 * Kafka Topic 名称常量。
 */
public final class KafkaTopic {

    private KafkaTopic() {
    }

    /** 温湿度遥测输入 Topic。 */
    public static final String DEVICE_TELEMETRY_TEMPERATURE = "device.telemetry.temperature";

    /** 门磁事件输入 Topic。 */
    public static final String DEVICE_EVENT_DOOR = "device.event.door";

    /** 人体感应事件输入 Topic。 */
    public static final String DEVICE_EVENT_MOTION = "device.event.motion";

    /** 分钟窗口房间温度统计输出 Topic。 */
    public static final String STREAM_ROOM_TEMP_STATS = "streams.room-temp-stats";

    /** 超温告警输出 Topic。 */
    public static final String STREAM_TEMP_THRESHOLD_ALERT = "streams.temp-threshold-alert";

    /** 入侵告警输出 Topic。 */
    public static final String STREAM_INTRUSION_ALERT = "streams.intrusion-alert";

    /** 输出日志 Consumer 专用 Group。 */
    public static final String STREAMS_OUTPUT_LOG_GROUP = "streams-output-log-group";
}
