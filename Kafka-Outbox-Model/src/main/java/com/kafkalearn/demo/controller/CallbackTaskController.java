package com.kafkalearn.demo.controller;

import com.kafkalearn.demo.api.request.CreateCallbackTaskRequest;
import com.kafkalearn.demo.api.response.CallbackConsumeLogResponse;
import com.kafkalearn.demo.api.response.CallbackTaskResponse;
import com.kafkalearn.demo.service.CallbackTaskService;
import jakarta.validation.Valid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/api/callback-demo/tasks")
public class CallbackTaskController {

    private static final Logger log = LoggerFactory.getLogger(CallbackTaskController.class);

    private final CallbackTaskService callbackTaskService;

    public CallbackTaskController(CallbackTaskService callbackTaskService) {
        this.callbackTaskService = callbackTaskService;
    }

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public CallbackTaskResponse createTask(@Valid @RequestBody CreateCallbackTaskRequest request) {
        log.info("Received create callback task request, bizNo={}, simulateMode={}", request.bizNo(), request.simulateMode());
        return callbackTaskService.createTask(request);
    }

    @GetMapping("/{taskId}")
    public CallbackTaskResponse getTask(@PathVariable UUID taskId) {
        log.info("Received query callback task request, taskId={}", taskId);
        return callbackTaskService.getTask(taskId);
    }

    @GetMapping("/{taskId}/logs")
    public List<CallbackConsumeLogResponse> getTaskLogs(@PathVariable UUID taskId) {
        log.info("Received query callback task logs request, taskId={}", taskId);
        return callbackTaskService.getTaskLogs(taskId);
    }
}
