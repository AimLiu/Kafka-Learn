package com.kafkalearn.demo.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafkalearn.demo.config.AppKafkaProperties;
import com.kafkalearn.demo.domain.OutboxStatus;
import com.kafkalearn.demo.domain.TaskStatus;
import com.kafkalearn.demo.entity.CallbackOutboxEntity;
import com.kafkalearn.demo.entity.CallbackTaskEntity;
import com.kafkalearn.demo.messaging.CallbackEventMessage;
import com.kafkalearn.demo.repository.CallbackOutboxRepository;
import com.kafkalearn.demo.repository.CallbackTaskRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Service
public class CallbackOutboxService {

    private static final Logger log = LoggerFactory.getLogger(CallbackOutboxService.class);

    private final CallbackOutboxRepository callbackOutboxRepository;
    private final CallbackTaskRepository callbackTaskRepository;
    private final KafkaTemplate<String, CallbackEventMessage> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final AppKafkaProperties appKafkaProperties;

    public CallbackOutboxService(CallbackOutboxRepository callbackOutboxRepository,
                                 CallbackTaskRepository callbackTaskRepository,
                                 KafkaTemplate<String, CallbackEventMessage> kafkaTemplate,
                                 ObjectMapper objectMapper,
                                 AppKafkaProperties appKafkaProperties) {
        this.callbackOutboxRepository = callbackOutboxRepository;
        this.callbackTaskRepository = callbackTaskRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.appKafkaProperties = appKafkaProperties;
    }

    /**
     * 为任务创建一条新的 Outbox 记录，真正的 Kafka 发布由定时任务异步完成。
     */
    public void createOutboxRecord(CallbackTaskEntity task, String reason) {
        LocalDateTime now = LocalDateTime.now();
        CallbackOutboxEntity outbox = new CallbackOutboxEntity();
        outbox.setId(UUID.randomUUID());
        outbox.setTask(task);
        outbox.setTopicName(task.getTopicName());
        outbox.setMessageKey(task.getId().toString());
        outbox.setMessagePayload(objectMapper.valueToTree(buildMessage(task)));
        outbox.setStatus(OutboxStatus.NEW);
        outbox.setPublishRetryCount(0);
        outbox.setNextAttemptTime(now);
        outbox.setLastErrorMsg(reason);
        outbox.setCreatedAt(now);
        outbox.setUpdatedAt(now);
        callbackOutboxRepository.save(outbox);
        log.info("Created outbox record, outboxId={}, taskId={}, reason={}", outbox.getId(), task.getId(), reason);
    }

    /**
     * 定时扫描待发布的 Outbox 记录，补偿 Kafka 投递失败场景。
     */
    @Scheduled(fixedDelayString = "${app.scheduler.outbox-delay-ms:5000}")
    @Transactional
    public void publishPendingOutboxes() {
        List<CallbackOutboxEntity> outboxes = callbackOutboxRepository
                .findTop20ByStatusInAndNextAttemptTimeLessThanEqualOrderByCreatedAtAsc(
                        List.of(OutboxStatus.NEW, OutboxStatus.PUBLISH_FAILED),
                        LocalDateTime.now()
                );
        if (outboxes.isEmpty()) {
            return;
        }

        log.info("Found pending outbox records, count={}", outboxes.size());
        for (CallbackOutboxEntity outbox : outboxes) {
            publishSingleOutbox(outbox.getId());
        }
    }

    /**
     * 发布单条 Outbox 消息，并回写 task/outbox 状态。
     */
    public void publishSingleOutbox(UUID outboxId) {
        callbackOutboxRepository.findById(outboxId).ifPresent(outbox -> {
            if (outbox.getStatus() == OutboxStatus.PUBLISHED) {
                log.info("Skip outbox publish because it is already published, outboxId={}", outboxId);
                return;
            }

            try {
                CallbackEventMessage message = objectMapper.treeToValue(
                        outbox.getMessagePayload(),
                        CallbackEventMessage.class
                );
                log.info("Publishing outbox message to Kafka, outboxId={}, taskId={}, topic={}",
                        outboxId, outbox.getTask().getId(), outbox.getTopicName());
                SendResult<String, CallbackEventMessage> result = kafkaTemplate.send(outbox.getTopicName(), outbox.getMessageKey(), message)
                        .get(appKafkaProperties.getSendTimeoutSeconds(), TimeUnit.SECONDS);
                log.info("kafka send msg return : [{}]", result);
                LocalDateTime now = LocalDateTime.now();
                outbox.setStatus(OutboxStatus.PUBLISHED);
                outbox.setLastErrorMsg(null);
                outbox.setUpdatedAt(now);
                callbackOutboxRepository.save(outbox);

                CallbackTaskEntity task = callbackTaskRepository.findById(outbox.getTask().getId())
                        .orElse(outbox.getTask());
                if (task.getStatus() == TaskStatus.CREATED) {
                    task.setStatus(TaskStatus.PUBLISHED);
                    task.setPublishTime(now);
                    task.setResultMsg("Outbox 已成功发布到 Kafka");
                    task.setUpdatedAt(now);
                    callbackTaskRepository.save(task);
                }
                log.info("Outbox publish succeeded, outboxId={}, taskId={}", outboxId, outbox.getTask().getId());
            } catch (Exception ex) {
                LocalDateTime nextAttemptTime = LocalDateTime.now().plusSeconds(calculateOutboxBackoffSeconds(outbox));
                outbox.setStatus(OutboxStatus.PUBLISH_FAILED);
                outbox.setPublishRetryCount(outbox.getPublishRetryCount() + 1);
                outbox.setNextAttemptTime(nextAttemptTime);
                outbox.setLastErrorMsg(trimMessage(ex.getMessage()));
                outbox.setUpdatedAt(LocalDateTime.now());
                callbackOutboxRepository.save(outbox);
                log.error("Outbox publish failed, outboxId={}, taskId={}, nextAttemptTime={}",
                        outboxId, outbox.getTask().getId(), nextAttemptTime, ex);
            }
        });
    }

    private CallbackEventMessage buildMessage(CallbackTaskEntity task) {
        return new CallbackEventMessage(
                task.getId(),
                task.getBizNo(),
                task.getTraceId(),
                task.getSimulateMode(),
                task.getCreatedAt()
        );
    }

    private long calculateOutboxBackoffSeconds(CallbackOutboxEntity outbox) {
        return Math.min(300L, 10L * Math.max(1, outbox.getPublishRetryCount() + 1L));
    }

    private String trimMessage(String message) {
        if (message == null || message.isBlank()) {
            return "未知异常";
        }
        return message.length() > 200 ? message.substring(0, 200) : message;
    }
}
