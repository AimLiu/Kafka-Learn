package com.kafkalearn.demo.domain;

public record NotifySimulationResult(boolean success, String message, long costMs) {
}
