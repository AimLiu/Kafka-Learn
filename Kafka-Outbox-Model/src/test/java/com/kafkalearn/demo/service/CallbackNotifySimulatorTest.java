package com.kafkalearn.demo.service;

import com.kafkalearn.demo.domain.NotifySimulationResult;
import com.kafkalearn.demo.domain.SimulateMode;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CallbackNotifySimulatorTest {

    private final CallbackNotifySimulator callbackNotifySimulator = new CallbackNotifySimulator();

    @Test
    void shouldReturnSuccessWhenModeIsSuccess() {
        NotifySimulationResult result = callbackNotifySimulator.simulate(SimulateMode.SUCCESS);
        assertTrue(result.success());
    }

    @Test
    void shouldReturnFailureWhenModeIsFail() {
        NotifySimulationResult result = callbackNotifySimulator.simulate(SimulateMode.FAIL);
        assertFalse(result.success());
    }
}
