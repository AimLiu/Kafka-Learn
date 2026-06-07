package com.kafkalearn.demo.service;

import com.kafkalearn.demo.domain.NotifySimulationResult;
import com.kafkalearn.demo.domain.SimulateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.concurrent.ThreadLocalRandom;

@Component
public class CallbackNotifySimulator {

    private static final Logger log = LoggerFactory.getLogger(CallbackNotifySimulator.class);

    public NotifySimulationResult simulate(SimulateMode simulateMode) {
        long costMs = ThreadLocalRandom.current().nextLong(20, 200);
        NotifySimulationResult result = switch (simulateMode) {
            case SUCCESS -> new NotifySimulationResult(true, "模拟通知成功", costMs);
            case FAIL -> new NotifySimulationResult(false, "模拟通知失败", costMs);
            case RANDOM -> ThreadLocalRandom.current().nextBoolean()
                    ? new NotifySimulationResult(true, "随机命中成功", costMs)
                    : new NotifySimulationResult(false, "随机命中失败", costMs);
        };
        log.info("Simulated callback result, mode={}, success={}, message={}, costMs={}",
                simulateMode, result.success(), result.message(), result.costMs());
        return result;
    }
}
