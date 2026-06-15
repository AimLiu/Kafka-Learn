package com.kafkalearn.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * SmartHomeSimulator 调度与场景开关。
 */
@Data
@Component
@ConfigurationProperties(prefix = "app.simulator")
public class SimulatorProperties {

    /** 是否启用定时模拟。 */
    private boolean enabled = true;

    /** 温湿度遥测发送间隔（毫秒）。 */
    private long telemetryIntervalMs = 5000;

    /** 是否模拟厨房超温（L2）。 */
    private boolean kitchenOverheatEnabled = true;

    /** 厨房超温模拟温度（摄氏度）。 */
    private double kitchenOverheatTemperature = 32.0;

    /** 应用就绪后是否自动跑一遍 L1–L6 + 异常样例（一次性）。 */
    private boolean scenarioDemoEnabled = true;

    /** 场景演示启动延迟（毫秒），等待 Kafka Streams 进入 RUNNING。 */
    private long scenarioStartupDelayMs = 12_000;

    /** Join 超时演示（L4）中 door 与 motion 的间隔（毫秒），应大于 intrusion-join-seconds。 */
    private long joinTimeoutDemoGapMs = 35_000;
}
