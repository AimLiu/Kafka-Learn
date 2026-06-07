package com.kafkalearn.demo.service;

import com.kafkalearn.demo.config.AppRetryProperties;
import com.kafkalearn.demo.domain.TaskStatus;
import com.kafkalearn.demo.entity.CallbackTaskEntity;
import com.kafkalearn.demo.repository.CallbackTaskRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;

@Service
public class CallbackRetryService {

    private static final Logger log = LoggerFactory.getLogger(CallbackRetryService.class);

    private final CallbackTaskRepository callbackTaskRepository;
    private final CallbackOutboxService callbackOutboxService;
    private final AppRetryProperties appRetryProperties;

    public CallbackRetryService(CallbackTaskRepository callbackTaskRepository,
                                CallbackOutboxService callbackOutboxService,
                                AppRetryProperties appRetryProperties) {
        this.callbackTaskRepository = callbackTaskRepository;
        this.callbackOutboxService = callbackOutboxService;
        this.appRetryProperties = appRetryProperties;
    }

    /**
     * 消费失败后，根据重试次数决定进入 RETRY_WAITING 或直接 FAILED。
     */
    public void handleConsumeFailure(CallbackTaskEntity task, String failureMessage) {
        if (task.getRetryCount() < task.getMaxRetryCount()) {
            int nextRetryCount = task.getRetryCount() + 1;
            LocalDateTime nextRetryTime = LocalDateTime.now().plusSeconds(resolveRetryDelaySeconds(nextRetryCount));
            task.setRetryCount(nextRetryCount);
            task.setStatus(TaskStatus.RETRY_WAITING);
            task.setNextRetryTime(nextRetryTime);
            task.setResultMsg(failureMessage + "，将在 " + nextRetryTime + " 重试");
            task.setUpdatedAt(LocalDateTime.now());
            callbackTaskRepository.save(task);
            log.warn("Callback task moved to retry waiting, taskId={}, retryCount={}, nextRetryTime={}",
                    task.getId(), task.getRetryCount(), task.getNextRetryTime());
            return;
        }

        task.setStatus(TaskStatus.FAILED);
        task.setNextRetryTime(null);
        task.setResultMsg(failureMessage + "，超过最大重试次数");
        task.setUpdatedAt(LocalDateTime.now());
        callbackTaskRepository.save(task);
        log.error("Callback task marked as failed after max retries, taskId={}, retryCount={}",
                task.getId(), task.getRetryCount());
    }

    /**
     * 扫描到期的 RETRY_WAITING 任务，重新写 Outbox 并回到 CREATED。
     */
    @Scheduled(fixedDelayString = "${app.scheduler.retry-delay-ms:10000}")
    @Transactional
    public void requeueRetryWaitingTasks() {
        List<CallbackTaskEntity> tasks = callbackTaskRepository
                .findTop20ByStatusAndNextRetryTimeLessThanEqualOrderByNextRetryTimeAsc(
                        TaskStatus.RETRY_WAITING,
                        LocalDateTime.now()
                );
        if (tasks.isEmpty()) {
            return;
        }

        log.info("Found retry waiting tasks ready to requeue, count={}", tasks.size());
        for (CallbackTaskEntity task : tasks) {
            task.setStatus(TaskStatus.CREATED);
            task.setNextRetryTime(null);
            task.setUpdatedAt(LocalDateTime.now());
            task.setResultMsg("任务已重新入队，等待 Outbox 发布");
            callbackTaskRepository.save(task);
            callbackOutboxService.createOutboxRecord(task, "失败重试重新入队");
            log.info("Retry waiting task requeued to outbox, taskId={}, retryCount={}",
                    task.getId(), task.getRetryCount());
        }
    }

    /**
     * 恢复长时间卡在 CONSUMING 的任务，避免因为消费者异常退出导致任务永久悬挂。
     */
    @Scheduled(fixedDelayString = "${app.scheduler.recovery-delay-ms:15000}")
    @Transactional
    public void recoverTimeoutConsumingTasks() {
        LocalDateTime timeoutPoint = LocalDateTime.now().minusSeconds(appRetryProperties.getConsumeTimeoutSeconds());
        List<CallbackTaskEntity> tasks = callbackTaskRepository
                .findTop20ByStatusAndUpdatedAtLessThanEqualOrderByUpdatedAtAsc(
                        TaskStatus.CONSUMING,
                        timeoutPoint
                );
        if (tasks.isEmpty()) {
            return;
        }

        log.warn("Found timeout consuming tasks, count={}, timeoutPoint={}", tasks.size(), timeoutPoint);
        for (CallbackTaskEntity task : tasks) {
            handleConsumeFailure(task, "消费者处理超时，已触发恢复补偿");
        }
    }

    private long resolveRetryDelaySeconds(int retryCount) {
        return switch (retryCount) {
            case 1 -> appRetryProperties.getFirstDelaySeconds();
            case 2 -> appRetryProperties.getSecondDelaySeconds();
            default -> appRetryProperties.getThirdDelaySeconds();
        };
    }
}
