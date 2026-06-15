package com.kafkalearn.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * Streams Topology 业务参数（阈值、窗口、Join 时长）。
 */
@Data
@Component
@ConfigurationProperties(prefix = "app.streams")
public class StreamsAppProperties {

    /** 超温告警阈值（摄氏度）。 */
    private double tempThresholdCelsius = 30.0;

    /** 温度聚合 Tumbling 窗口长度（分钟）。 */
    private int tempWindowMinutes = 1;

    /** 门磁与人体事件 Join 时间差上限（秒）。 */
    private int intrusionJoinSeconds = 30;
}
