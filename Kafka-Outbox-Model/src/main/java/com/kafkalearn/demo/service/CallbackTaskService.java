package com.kafkalearn.demo.service;

import com.kafkalearn.demo.api.request.CreateCallbackTaskRequest;
import com.kafkalearn.demo.api.response.CallbackConsumeLogResponse;
import com.kafkalearn.demo.api.response.CallbackTaskResponse;
import com.kafkalearn.demo.config.AppKafkaProperties;
import com.kafkalearn.demo.config.AppRetryProperties;
import com.kafkalearn.demo.domain.TaskStatus;
import com.kafkalearn.demo.entity.CallbackConsumeLogEntity;
import com.kafkalearn.demo.entity.CallbackTaskEntity;
import com.kafkalearn.demo.exception.ResourceNotFoundException;
import com.kafkalearn.demo.repository.CallbackConsumeLogRepository;
import com.kafkalearn.demo.repository.CallbackTaskRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

@Service
public class CallbackTaskService {

    private static final Logger log = LoggerFactory.getLogger(CallbackTaskService.class);

    private final CallbackTaskRepository callbackTaskRepository;
    private final CallbackConsumeLogRepository callbackConsumeLogRepository;
    private final AppKafkaProperties appKafkaProperties;
    private final AppRetryProperties appRetryProperties;
    private final CallbackOutboxService callbackOutboxService;

    public CallbackTaskService(CallbackTaskRepository callbackTaskRepository,
                               CallbackConsumeLogRepository callbackConsumeLogRepository,
                               AppKafkaProperties appKafkaProperties,
                               AppRetryProperties appRetryProperties,
                               CallbackOutboxService callbackOutboxService) {
        this.callbackTaskRepository = callbackTaskRepository;
        this.callbackConsumeLogRepository = callbackConsumeLogRepository;
        this.appKafkaProperties = appKafkaProperties;
        this.appRetryProperties = appRetryProperties;
        this.callbackOutboxService = callbackOutboxService;
    }

    /**
     * 创建任务时只做数据库本地事务：
     * 1. 写 callback_task
     * 2. 写 callback_outbox
     * 真正发 Kafka 交给 Outbox 调度器异步执行。
     */
    @Transactional
    public CallbackTaskResponse createTask(CreateCallbackTaskRequest request) {
        LocalDateTime now = LocalDateTime.now();
        CallbackTaskEntity entity = new CallbackTaskEntity();
        entity.setId(UUID.randomUUID());
        entity.setBizNo(request.bizNo());
        entity.setTopicName(appKafkaProperties.getCallbackTopic());
        entity.setPayload(request.payload());
        entity.setSimulateMode(request.simulateMode());
        entity.setStatus(TaskStatus.CREATED);
        entity.setTraceId(UUID.randomUUID().toString().replace("-", ""));
        entity.setRetryCount(0);
        entity.setMaxRetryCount(appRetryProperties.getMaxRetryCount());
        entity.setNextRetryTime(null);
        entity.setCreatedAt(now);
        entity.setUpdatedAt(now);

        log.info("Creating callback task, taskId={}, bizNo={}, topic={}, simulateMode={}",
                entity.getId(), entity.getBizNo(), entity.getTopicName(), entity.getSimulateMode());
        callbackTaskRepository.saveAndFlush(entity);
        log.info("Callback task persisted, taskId={}, status={}", entity.getId(), entity.getStatus());

        callbackOutboxService.createOutboxRecord(entity, "初次创建任务");
        log.info("Outbox record created for callback task, taskId={}", entity.getId());
        return getTask(entity.getId());
    }

    public CallbackTaskResponse getTask(UUID taskId) {
        log.debug("Loading callback task, taskId={}", taskId);
        CallbackTaskEntity entity = callbackTaskRepository.findById(taskId)
                .orElseThrow(() -> new ResourceNotFoundException("任务不存在: " + taskId));
        return toTaskResponse(entity);
    }

    public List<CallbackConsumeLogResponse> getTaskLogs(UUID taskId) {
        log.debug("Loading callback task logs, taskId={}", taskId);
        if (!callbackTaskRepository.existsById(taskId)) {
            throw new ResourceNotFoundException("任务不存在: " + taskId);
        }
        return callbackConsumeLogRepository.findByTask_IdOrderByCreatedAtAsc(taskId).stream()
                .map(this::toLogResponse)
                .toList();
    }

    private CallbackTaskResponse toTaskResponse(CallbackTaskEntity entity) {
        return new CallbackTaskResponse(
                entity.getId(),
                entity.getBizNo(),
                entity.getTopicName(),
                entity.getPayload(),
                entity.getSimulateMode(),
                entity.getStatus(),
                entity.getTraceId(),
                entity.getPublishTime(),
                entity.getConsumeTime(),
                entity.getRetryCount(),
                entity.getMaxRetryCount(),
                entity.getNextRetryTime(),
                entity.getResultMsg(),
                entity.getCreatedAt(),
                entity.getUpdatedAt()
        );
    }

    private CallbackConsumeLogResponse toLogResponse(CallbackConsumeLogEntity entity) {
        return new CallbackConsumeLogResponse(
                entity.getId(),
                entity.getTask().getId(),
                entity.getConsumerGroup(),
                entity.getTopicName(),
                entity.getPartitionNo(),
                entity.getOffsetNo(),
                entity.getSimulateMode(),
                entity.getConsumeResult(),
                entity.getErrorMsg(),
                entity.getCreatedAt()
        );
    }

    private String trimMessage(String message) {
        if (message == null || message.isBlank()) {
            return "未知异常";
        }
        return message.length() > 200 ? message.substring(0, 200) : message;
    }
}
