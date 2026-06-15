package com.kafkalearn.domain;

/**
 * 智慧家庭预置设备与房间常量。
 */
public final class SmartHomeConstants {

    private SmartHomeConstants() {
    }

    /** 演示家庭 ID。 */
    public static final String HOME_ID = "home-001";

    /** 客厅温湿度传感器。 */
    public static final String SENSOR_LIVING = "sensor-living-temp";

    /** 卧室温湿度传感器。 */
    public static final String SENSOR_BEDROOM = "sensor-bedroom-temp";

    /** 厨房温湿度传感器。 */
    public static final String SENSOR_KITCHEN = "sensor-kitchen-temp";

    /** 客厅房间 ID。 */
    public static final String ROOM_LIVING = "room-living";

    /** 卧室房间 ID。 */
    public static final String ROOM_BEDROOM = "room-bedroom";

    /** 厨房房间 ID。 */
    public static final String ROOM_KITCHEN = "room-kitchen";

    /** 入户门磁。 */
    public static final String DOOR_ENTRY = "door-entry";

    /** 客厅人体感应。 */
    public static final String MOTION_LIVING = "motion-living";
}
