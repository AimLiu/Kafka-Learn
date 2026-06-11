package com.kafkalearn.shedule;

import com.kafkalearn.config.SimulatorProperties;
import com.kafkalearn.event.common.AlertType;
import com.kafkalearn.event.common.Severity;
import com.kafkalearn.event.common.SimulateMode;
import com.kafkalearn.producer.EventPublisher;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

class DeviceAlertSimulatorTest {

    private DeviceAlertSimulator simulator;

    @BeforeEach
    void setUp() {
        SimulatorProperties props = new SimulatorProperties();
        simulator = new DeviceAlertSimulator(mock(EventPublisher.class), props);
    }

    @Test
    void buildFailPushAlert_shouldSetSimulateModeFailPush() {
        var event = simulator.buildFailPushAlert();
        assertEquals(SimulateMode.FAIL_PUSH, event.simulateMode());
        assertEquals(Severity.CRITICAL, event.severity());
    }

    @Test
    void buildTempThresholdAlert_shouldHaveMetricAbove80() {
        var event = simulator.buildTempThresholdAlert();
        assertEquals(AlertType.TEMP_HIGH, event.alertType());
        assertNotNull(event.metricValue());
        assertTrue(event.metricValue() > 80);
        assertEquals(SimulateMode.SUCCESS, event.simulateMode());
    }

    @Test
    void buildNoRuleHitAlert_shouldBeInfoWithLowMetric() {
        var event = simulator.buildNoRuleHitAlert();
        assertEquals(Severity.INFO, event.severity());
        assertEquals(70.0, event.metricValue());
    }

    @Test
    void buildRandomSuccessAlert_shouldUseSuccessMode() {
        var event = simulator.buildRandomSuccessAlert();
        assertEquals(SimulateMode.SUCCESS, event.simulateMode());
        assertNotNull(event.eventId());
        assertEquals("device-alert-simulator", event.source());
    }

    @Test
    void buildFailStorageAlert_shouldSetSimulateModeFailStorage() {
        assertEquals(SimulateMode.FAIL_STORAGE, simulator.buildFailStorageAlert().simulateMode());
    }

    @Test
    void buildFailRuleAlert_shouldSetSimulateModeFailRule() {
        assertEquals(SimulateMode.FAIL_RULE, simulator.buildFailRuleAlert().simulateMode());
    }

    @Test
    void buildCriticalPushAlert_shouldUseFixedDeviceIdFromProperties() {
        SimulatorProperties props = new SimulatorProperties();
        props.setFixedDeviceId(java.util.UUID.fromString("11111111-1111-1111-1111-111111111111"));
        DeviceAlertSimulator sim = new DeviceAlertSimulator(mock(EventPublisher.class), props);
        assertEquals(props.getFixedDeviceId(), sim.buildCriticalPushAlert().deviceId());
    }
}
