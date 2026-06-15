package com.kafkalearn.simulator;

import com.kafkalearn.config.KafkaTopic;
import com.kafkalearn.config.SimulatorProperties;
import com.kafkalearn.domain.SmartHomeConstants;
import com.kafkalearn.message.DoorEvent;
import com.kafkalearn.message.MotionEvent;
import com.kafkalearn.message.TemperatureReading;
import com.kafkalearn.producer.InputEventPublisher;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.concurrent.ThreadLocalRandom;

/**
 * 智慧家庭 IoT 模拟器：周期性遥测 + 启动时一次性跑完 L1–L6 与异常样例。
 *
 * <p>输入 Topic 有消息 ≠ 输出 Topic 有消息；输出由 Kafka Streams Topology 计算写入。
 * 本类负责把<strong>全部学习样例</strong>（含应被丢弃的异常数据）写入输入 Topic。
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class SmartHomeSimulator {

    private static final String DOOR_CLOSED = "CLOSED";

    private final SimulatorProperties simulatorProperties;
    private final InputEventPublisher inputEventPublisher;
    private final TaskScheduler taskScheduler;

    /**
     * 应用就绪后延迟触发一次性场景演示（L3–L6 + 异常样例）。
     */
    @EventListener(ApplicationReadyEvent.class)
    public void scheduleScenarioDemo() {
        if (!simulatorProperties.isEnabled() || !simulatorProperties.isScenarioDemoEnabled()) {
            return;
        }
        long delayMs = simulatorProperties.getScenarioStartupDelayMs();
        log.info("[SIM] {} ms 后执行 L1–L6 与异常样例演示（等待 Streams RUNNING）", delayMs);
        taskScheduler.schedule(this::runScenarioDemo, Instant.now().plusMillis(delayMs));
    }

    /**
     * 周期性发送 L1 正常温度与 L2 厨房超温（持续产生窗口聚合输入）。
     */
    @Scheduled(fixedDelayString = "${app.simulator.telemetry-interval-ms}")
    public void simulatePeriodicTelemetry() {
        if (!simulatorProperties.isEnabled()) {
            return;
        }
        long now = System.currentTimeMillis();
        publishLivingRoom(now, 24.0);
        publishBedroom(now, 22.0);
        if (simulatorProperties.isKitchenOverheatEnabled()) {
            publishKitchenOverheat(now);
        }
    }

    private void runScenarioDemo() {
        log.info("[SIM] ========== 开始 L1–L6 + 异常样例 ==========");
        long base = System.currentTimeMillis();

        runL1ExplicitSamples(base);
        runL2ExplicitOverheat(base + 500);
        runAbnormalTemperatureSamples(base + 1_000);
        runL6WindowBurst(base + 2_000);
        runL3IntrusionSuccess(base + 5_000);
        runL5DoorOnly(base + 8_000);
        runAbnormalDoorMotionSamples(base + 10_000);
        scheduleL4JoinTimeoutDemo(base + 12_000);

        log.info("[SIM] ========== 一次性演示已发送；L4 将在 {} ms 后完成 ==========",
                simulatorProperties.getJoinTimeoutDemoGapMs());
    }

    /** L1：显式发送正常聚合样例（客厅 / 卧室）。 */
    private void runL1ExplicitSamples(long reportedAt) {
        log.info("[SIM-L1] 正常聚合样例 → 期望 [STATS] room-living / room-bedroom");
        publishLivingRoom(reportedAt, 23.5);
        publishBedroom(reportedAt, 21.0);
    }

    /** L2：显式厨房超温样例。 */
    private void runL2ExplicitOverheat(long reportedAt) {
        log.info("[SIM-L2] 厨房超温样例 → 期望 [TEMP-ALERT] room-kitchen");
        publishKitchenOverheat(reportedAt);
    }

    /**
     * 异常温度样例：应被 {@link com.kafkalearn.topology.RoomTemperatureTopology} 丢弃，
     * 不应出现在 streams.* 输出 Topic。
     */
    private void runAbnormalTemperatureSamples(long reportedAt) {
        log.info("[SIM-ABNORMAL] 异常温度/非法 JSON → 期望无 streams 输出（随后可能出现反序列化 WARN，属预期）");

        publishTemperature(new TemperatureReading(
                "sensor-abnormal-cold",
                SmartHomeConstants.ROOM_LIVING,
                SmartHomeConstants.HOME_ID,
                -50.0,
                50.0,
                reportedAt
        ));

        publishTemperature(new TemperatureReading(
                "sensor-abnormal-hot",
                SmartHomeConstants.ROOM_KITCHEN,
                SmartHomeConstants.HOME_ID,
                85.0,
                50.0,
                reportedAt + 100
        ));

        publishTemperature(new TemperatureReading(
                "sensor-null-temp",
                SmartHomeConstants.ROOM_BEDROOM,
                SmartHomeConstants.HOME_ID,
                null,
                50.0,
                reportedAt + 200
        ));

        log.info("[SIM-ABNORMAL] 故意发送残缺 JSON（温度）→ 预期 EventJsonCodec WARN + Topology 丢弃");
        inputEventPublisher.publishRawAsync(
                KafkaTopic.DEVICE_TELEMETRY_TEMPERATURE,
                "sensor-malformed-json",
                "{deviceId:\"broken\", temperatureCelsius:25"
        );

        inputEventPublisher.publishRawAsync(
                KafkaTopic.DEVICE_TELEMETRY_TEMPERATURE,
                "sensor-missing-room",
                """
                        {"deviceId":"sensor-no-room","homeId":"home-001",\
                        "temperatureCelsius":25.0,"humidityPercent":50,"reportedAt":%d}\
                        """.formatted(reportedAt + 300)
        );
    }

    /** L6：同一房间 1 分钟窗口内连发 3 条，期望 sampleCount=3。 */
    private void runL6WindowBurst(long firstReportedAt) {
        log.info("[SIM-L6] 同房间 3 条温度 → 期望 [STATS] room-living sampleCount=3");
        for (int i = 0; i < 3; i++) {
            publishLivingRoom(firstReportedAt + i * 2_000L, 24.0 + i * 0.5);
        }
    }

    /** L3：门开 + 1 秒内人体 → 期望 [INTRUSION]。 */
    private void runL3IntrusionSuccess(long doorAt) {
        log.info("[SIM-L3] 门开 + 1s 内人体 → 期望 [INTRUSION] home=home-001");
        publishDoorOpen(doorAt);
        publishMotionDetected(doorAt + 1_000L);
    }

    /** L5：仅门开，无人体 → 期望无 [INTRUSION]。 */
    private void runL5DoorOnly(long doorAt) {
        log.info("[SIM-L5] 仅门开 → 期望无 [INTRUSION]");
        publishDoorOpen(doorAt);
    }

    /** L4：门开与人体间隔超过 Join 窗口 → 期望无 [INTRUSION]。 */
    private void scheduleL4JoinTimeoutDemo(long doorAt) {
        long gapMs = simulatorProperties.getJoinTimeoutDemoGapMs();
        log.info("[SIM-L4] 门开，{} ms 后人体 → 期望无 [INTRUSION]（Join 超时）", gapMs);
        publishDoorOpen(doorAt);
        taskScheduler.schedule(
                () -> publishMotionDetected(System.currentTimeMillis()),
                Instant.now().plusMillis(gapMs)
        );
    }

    /** 异常门磁/人体样例：CLOSED、detected=false → Join 流水线应忽略。 */
    private void runAbnormalDoorMotionSamples(long occurredAt) {
        log.info("[SIM-ABNORMAL] 门 CLOSED / 无人体 / 残缺 JSON → 期望无 [INTRUSION]（残缺 JSON 的 WARN 属预期）");

        DoorEvent closed = new DoorEvent(
                SmartHomeConstants.DOOR_ENTRY,
                SmartHomeConstants.HOME_ID,
                DOOR_CLOSED,
                occurredAt
        );
        inputEventPublisher.publishAsync(KafkaTopic.DEVICE_EVENT_DOOR, closed.homeId(), closed);

        MotionEvent noMotion = new MotionEvent(
                SmartHomeConstants.MOTION_LIVING,
                SmartHomeConstants.HOME_ID,
                SmartHomeConstants.ROOM_LIVING,
                false,
                occurredAt + 500
        );
        inputEventPublisher.publishAsync(KafkaTopic.DEVICE_EVENT_MOTION, noMotion.homeId(), noMotion);

        log.info("[SIM-ABNORMAL] 故意发送残缺 JSON（门磁）→ 预期 EventJsonCodec WARN + Topology 丢弃");
        inputEventPublisher.publishRawAsync(
                KafkaTopic.DEVICE_EVENT_DOOR,
                SmartHomeConstants.HOME_ID,
                "{\"deviceId\":\"door-bad\",\"state\":\"OPEN\""
        );
    }

    private void publishLivingRoom(long reportedAt, double temperature) {
        publishTemperature(new TemperatureReading(
                SmartHomeConstants.SENSOR_LIVING,
                SmartHomeConstants.ROOM_LIVING,
                SmartHomeConstants.HOME_ID,
                temperature,
                randomBetween(40.0, 60.0),
                reportedAt
        ));
    }

    private void publishBedroom(long reportedAt, double temperature) {
        publishTemperature(new TemperatureReading(
                SmartHomeConstants.SENSOR_BEDROOM,
                SmartHomeConstants.ROOM_BEDROOM,
                SmartHomeConstants.HOME_ID,
                temperature,
                randomBetween(40.0, 60.0),
                reportedAt
        ));
    }

    private void publishKitchenOverheat(long reportedAt) {
        publishTemperature(new TemperatureReading(
                SmartHomeConstants.SENSOR_KITCHEN,
                SmartHomeConstants.ROOM_KITCHEN,
                SmartHomeConstants.HOME_ID,
                simulatorProperties.getKitchenOverheatTemperature(),
                45.0,
                reportedAt
        ));
    }

    private void publishTemperature(TemperatureReading reading) {
        inputEventPublisher.publishAsync(
                KafkaTopic.DEVICE_TELEMETRY_TEMPERATURE,
                reading.deviceId(),
                reading
        );
    }

    private void publishDoorOpen(long occurredAt) {
        DoorEvent door = new DoorEvent(
                SmartHomeConstants.DOOR_ENTRY,
                SmartHomeConstants.HOME_ID,
                DoorEvent.STATE_OPEN,
                occurredAt
        );
        inputEventPublisher.publishAsync(KafkaTopic.DEVICE_EVENT_DOOR, door.homeId(), door);
    }

    private void publishMotionDetected(long occurredAt) {
        MotionEvent motion = new MotionEvent(
                SmartHomeConstants.MOTION_LIVING,
                SmartHomeConstants.HOME_ID,
                SmartHomeConstants.ROOM_LIVING,
                true,
                occurredAt
        );
        inputEventPublisher.publishAsync(KafkaTopic.DEVICE_EVENT_MOTION, motion.homeId(), motion);
    }

    private double randomBetween(double min, double max) {
        return ThreadLocalRandom.current().nextDouble(min, max);
    }
}
