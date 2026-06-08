package com.kafkalearn.producer;

import com.kafkalearn.event.DeviceStatusChangedEvent;
import org.springframework.kafka.support.SendResult;

import java.util.concurrent.CompletableFuture;

public interface EventPublisher {

    /**
     * 异步通知事件
     * @param event 通知事件
     * @return
     */
    CompletableFuture<SendResult<String, String>> publishAsync(DeviceStatusChangedEvent event);

    /**
     * 同步通知事件
     * @param event 通知事件
     * @return
     */
    SendResult<String, String> publishSync(DeviceStatusChangedEvent event);
}
