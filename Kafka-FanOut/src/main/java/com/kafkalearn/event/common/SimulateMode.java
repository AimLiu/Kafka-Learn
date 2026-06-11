package com.kafkalearn.event.common;

public enum SimulateMode {
    // 三个下游正常处理
    SUCCESS,
    // 仅 Storage Consumer 抛异常
    FAIL_STORAGE,
    // 仅 Push Consumer 抛异常
    FAIL_PUSH,
    // 仅 Rule Consumer 抛异常
    FAIL_RULE
}
