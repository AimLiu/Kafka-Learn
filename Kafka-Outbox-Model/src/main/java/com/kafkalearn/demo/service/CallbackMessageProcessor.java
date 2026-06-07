package com.kafkalearn.demo.service;

import com.kafkalearn.demo.domain.ConsumeResult;
import com.kafkalearn.demo.domain.NotifySimulationResult;
import com.kafkalearn.demo.domain.TaskStatus;
import com.kafkalearn.demo.entity.CallbackConsumeLogEntity;
import com.kafkalearn.demo.entity.CallbackTaskEntity;
import com.kafkalearn.demo.messaging.CallbackEventMessage;
import com.kafkalearn.demo.repository.CallbackConsumeLogRepository;
import com.kafkalearn.demo.repository.CallbackTaskRepository;
import jakarta.transaction.Transactional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.Optional;
import java.util.UUID;

@Service
public class CallbackMessageProcessor {

    private static final Logger log = LoggerFactory.getLogger(CallbackMessageProcessor.class);

    private final CallbackTaskRepository callbackTaskRepository;
    private final CallbackConsumeLogRepository callbackConsumeLogRepository;
    private final CallbackNotifySimulator callbackNotifySimulator;
    private final CallbackRetryService callbackRetryService;

    @Value("${spring.kafka.consumer.group-id}")
    private String consumerGroupId;

    public CallbackMessageProcessor(CallbackTaskRepository callbackTaskRepository,
                                    CallbackConsumeLogRepository callbackConsumeLogRepository,
                                    CallbackNotifySimulator callbackNotifySimulator,
                                    CallbackRetryService callbackRetryService) {
        this.callbackTaskRepository = callbackTaskRepository;
        this.callbackConsumeLogRepository = callbackConsumeLogRepository;
        this.callbackNotifySimulator = callbackNotifySimulator;
        this.callbackRetryService = callbackRetryService;
    }

    /**
     * 真正的消费处理逻辑放在独立 Bean 中，确保 @Transactional 通过 Spring 代理生效。
     */
    @Transactional
    public void processMessage(CallbackEventMessage message, String topic, int partition, long offset) {
        Optional<CallbackTaskEntity> optionalTask = callbackTaskRepository.findById(message.taskId());
        if (optionalTask.isEmpty()) {
            log.warn("Callback task not found, taskId={}", message.taskId());
            return;
        }

        CallbackTaskEntity task = optionalTask.get();
        //尝试更新task表状态
        int updated = tryClaim(task.getId());
        if (updated == 0) {
            log.warn("Skip callback task consume because status is not claimable, taskId={}, currentStatus={}",
                    task.getId(), task.getStatus());
            saveLog(task, topic, partition, offset, ConsumeResult.IGNORED, "任务状态不是 CREATED/PUBLISHED，跳过重复消费");
            return;
        }

        CallbackTaskEntity currentTask = callbackTaskRepository.findById(task.getId()).orElse(task);
        log.info("Callback task claimed by consumer, taskId={}, status={}, consumerGroup={}",
                currentTask.getId(), TaskStatus.CONSUMING, consumerGroupId);
        //进行消费通知后，将结果进行返回
        NotifySimulationResult result = callbackNotifySimulator.simulate(currentTask.getSimulateMode());
        saveLog(
                currentTask,
                topic,
                partition,
                offset,
                result.success() ? ConsumeResult.SUCCESS : ConsumeResult.FAILED,
                result.message()
        );

        currentTask.setConsumeTime(LocalDateTime.now());
        currentTask.setUpdatedAt(LocalDateTime.now());

        if (result.success()) {
            currentTask.setStatus(TaskStatus.SUCCESS);
            currentTask.setNextRetryTime(null);
            currentTask.setResultMsg(result.message() + "，耗时 " + result.costMs() + " ms");
            callbackTaskRepository.save(currentTask);
            log.info("Callback task consume finished, taskId={}, finalStatus={}, resultMessage={}",
                    currentTask.getId(), currentTask.getStatus(), currentTask.getResultMsg());
            return;
        }

        callbackRetryService.handleConsumeFailure(currentTask, result.message() + "，耗时 " + result.costMs() + " ms");
    }

    private int tryClaim(UUID taskId) {
        int updated = callbackTaskRepository.claimStatus(
                taskId,
                TaskStatus.PUBLISHED,
                TaskStatus.CONSUMING,
                LocalDateTime.now()
        );
        if (updated > 0) {
            log.info("Claim callback task from PUBLISHED to CONSUMING, taskId={}", taskId);
        }
        if (updated == 0) {
            updated = callbackTaskRepository.claimStatus(
                    taskId,
                    TaskStatus.CREATED,
                    TaskStatus.CONSUMING,
                    LocalDateTime.now()
            );
            if (updated > 0) {
                log.info("Claim callback task from CREATED to CONSUMING, taskId={}", taskId);
            }
        }
        return updated;
    }

    private void saveLog(CallbackTaskEntity task,
                         String topic,
                         int partition,
                         long offset,
                         ConsumeResult consumeResult,
                         String errorMessage) {
        CallbackConsumeLogEntity logEntity = new CallbackConsumeLogEntity();
        logEntity.setId(UUID.randomUUID());
        logEntity.setTask(task);
        logEntity.setConsumerGroup(consumerGroupId);
        logEntity.setTopicName(topic);
        logEntity.setPartitionNo(partition);
        logEntity.setOffsetNo(offset);
        logEntity.setSimulateMode(task.getSimulateMode());
        logEntity.setConsumeResult(consumeResult);
        logEntity.setErrorMsg(errorMessage);
        logEntity.setCreatedAt(LocalDateTime.now());
        callbackConsumeLogRepository.save(logEntity);
        log.info("Callback consume log persisted, taskId={}, consumeResult={}, topic={}, partition={}, offset={}",
                task.getId(), consumeResult, topic, partition, offset);
    }
}
