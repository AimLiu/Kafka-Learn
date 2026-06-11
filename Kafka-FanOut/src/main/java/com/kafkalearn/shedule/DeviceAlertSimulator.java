package com.kafkalearn.shedule;

import com.kafkalearn.config.SimulatorProperties;
import com.kafkalearn.event.DeviceAlertTriggeredEvent;
import com.kafkalearn.event.common.AlertType;
import com.kafkalearn.event.common.Severity;
import com.kafkalearn.event.common.SimulateMode;
import com.kafkalearn.producer.EventPublisher;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@Service
@RequiredArgsConstructor
public class DeviceAlertSimulator {

    private static final String SOURCE = "device-alert-simulator";

    private static final UUID[] DEVICE_IDS = {
            UUID.fromString("c542567a-dfb1-4af7-940a-a5cada4372b6"),
            UUID.fromString("26ad1ca1-d0d4-4dff-b609-b199069f4f0d"),
            UUID.fromString("cf935c51-5bbe-4743-95b9-db13e89b5006")
    };

    private final EventPublisher eventPublisher;
    private final SimulatorProperties properties;
    private final Random random = new Random();
    private final AtomicInteger learningScenarioIndex = new AtomicInteger(0);

    @Scheduled(fixedRateString = "${app.simulator.random-interval-ms:5000}")
    public void simulateRandomSuccess() {
        publish(buildRandomSuccessAlert());
    }

    @Scheduled(fixedRateString = "${app.simulator.learning-cycle-interval-ms:30000}")
    public void simulateLearningCycle() {
        if (!properties.isLearningCycleEnabled()) {
            return;
        }
        publish(nextLearningScenario());
    }

    private void publish(DeviceAlertTriggeredEvent event) {
        log.info("Simulator 发布告警, eventId={}, mode={}, type={}, severity={}, metric={}",
                event.eventId(), event.simulateMode(), event.alertType(),
                event.severity(), event.metricValue());
        eventPublisher.publishSync(event);
    }

    private DeviceAlertTriggeredEvent nextLearningScenario() {
        LearningScenario[] scenarios = LearningScenario.values();
        int idx = Math.floorMod(learningScenarioIndex.getAndIncrement(), scenarios.length);
        return scenarios[idx].toEvent(this);
    }

    public DeviceAlertTriggeredEvent buildRandomSuccessAlert() {
        AlertType type = AlertType.values()[random.nextInt(AlertType.values().length)];
        Severity severity = Severity.values()[random.nextInt(Severity.values().length)];
        Double metric = type == AlertType.TEMP_HIGH ? 60 + random.nextDouble() * 40 : null;
        return newEvent(pickRandomDeviceId(), type, severity, metric, SimulateMode.SUCCESS);
    }

    public DeviceAlertTriggeredEvent buildCriticalPushAlert() {
        return newEvent(pickFixedDeviceId(), AlertType.SMOKE, Severity.CRITICAL, null, SimulateMode.SUCCESS);
    }

    public DeviceAlertTriggeredEvent buildAppPushAlert() {
        return newEvent(pickFixedDeviceId(), AlertType.TEMP_HIGH, Severity.WARN, 72.0, SimulateMode.SUCCESS);
    }

    public DeviceAlertTriggeredEvent buildCriticalRuleAlert() {
        return newEvent(pickFixedDeviceId(), AlertType.TEMP_HIGH, Severity.CRITICAL, 90.0, SimulateMode.SUCCESS);
    }

    public DeviceAlertTriggeredEvent buildTempThresholdAlert() {
        return newEvent(pickFixedDeviceId(), AlertType.TEMP_HIGH, Severity.WARN, 92.5, SimulateMode.SUCCESS);
    }

    public DeviceAlertTriggeredEvent buildNoRuleHitAlert() {
        return newEvent(pickFixedDeviceId(), AlertType.TEMP_HIGH, Severity.INFO, 70.0, SimulateMode.SUCCESS);
    }

    public DeviceAlertTriggeredEvent buildFailStorageAlert() {
        return newEvent(pickFixedDeviceId(), AlertType.OFFLINE, Severity.WARN, null, SimulateMode.FAIL_STORAGE);
    }

    public DeviceAlertTriggeredEvent buildFailPushAlert() {
        return newEvent(pickFixedDeviceId(), AlertType.TEMP_HIGH, Severity.CRITICAL, 88.0, SimulateMode.FAIL_PUSH);
    }

    public DeviceAlertTriggeredEvent buildFailRuleAlert() {
        return newEvent(pickFixedDeviceId(), AlertType.SMOKE, Severity.WARN, null, SimulateMode.FAIL_RULE);
    }

    public DeviceAlertTriggeredEvent buildSmokeAlert() {
        return newEvent(pickRandomDeviceId(), AlertType.SMOKE, Severity.WARN, null, SimulateMode.SUCCESS);
    }

    public DeviceAlertTriggeredEvent buildOfflineAlert() {
        return newEvent(pickRandomDeviceId(), AlertType.OFFLINE, Severity.INFO, null, SimulateMode.SUCCESS);
    }

    private DeviceAlertTriggeredEvent newEvent(
            UUID deviceId,
            AlertType alertType,
            Severity severity,
            Double metricValue,
            SimulateMode simulateMode) {
        return new DeviceAlertTriggeredEvent(
                UUID.randomUUID().toString(),
                System.currentTimeMillis(),
                deviceId,
                alertType,
                severity,
                metricValue,
                SOURCE,
                simulateMode
        );
    }

    private UUID pickRandomDeviceId() {
        return DEVICE_IDS[random.nextInt(DEVICE_IDS.length)];
    }

    private UUID pickFixedDeviceId() {
        return properties.getFixedDeviceId();
    }

    private enum LearningScenario {
        CRITICAL_PUSH, APP_PUSH, CRITICAL_RULE, TEMP_THRESHOLD, NO_RULE_HIT,
        FAIL_STORAGE, FAIL_PUSH, FAIL_RULE, SMOKE, OFFLINE;

        DeviceAlertTriggeredEvent toEvent(DeviceAlertSimulator sim) {
            return switch (this) {
                case CRITICAL_PUSH -> sim.buildCriticalPushAlert();
                case APP_PUSH -> sim.buildAppPushAlert();
                case CRITICAL_RULE -> sim.buildCriticalRuleAlert();
                case TEMP_THRESHOLD -> sim.buildTempThresholdAlert();
                case NO_RULE_HIT -> sim.buildNoRuleHitAlert();
                case FAIL_STORAGE -> sim.buildFailStorageAlert();
                case FAIL_PUSH -> sim.buildFailPushAlert();
                case FAIL_RULE -> sim.buildFailRuleAlert();
                case SMOKE -> sim.buildSmokeAlert();
                case OFFLINE -> sim.buildOfflineAlert();
            };
        }
    }
}
