package com.kafkalearn.producer;

import com.kafkalearn.messaging.EventJsonCodec;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

/**
 * 向 Kafka 输入 Topic 发送 JSON 事件，供 Simulator 与 HTTP 注入共用。
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class InputEventPublisher {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final EventJsonCodec eventJsonCodec;

    /**
     * 异步发送事件到指定 Topic。
     *
     * @param topic 目标 Topic
     * @param key   Record Key
     * @param event 消息对象
     * @return 发送 Future
     */
    public CompletableFuture<SendResult<String, String>> publishAsync(String topic, String key, Object event) {
        String payload = eventJsonCodec.serialize(event);
        log.info("发送事件, topic={}, key={}, payload={}", topic, key, payload);
        return kafkaTemplate.send(topic, key, payload);
    }

    /**
     * 发送原始 JSON 字符串，用于模拟非法/残缺 payload（异常数据学习样例）。
     *
     * @param topic   目标 Topic
     * @param key     Record Key
     * @param payload 原始 JSON 或非法字符串
     * @return 发送 Future
     */
    public CompletableFuture<SendResult<String, String>> publishRawAsync(String topic, String key, String payload) {
        log.info("发送原始事件(可能异常), topic={}, key={}, payload={}", topic, key, payload);
        return kafkaTemplate.send(topic, key, payload);
    }
}
