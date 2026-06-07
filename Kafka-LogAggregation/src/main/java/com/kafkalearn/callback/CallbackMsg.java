package com.kafkalearn.callback;

import com.kafkalearn.msg.KafkaSendMsg;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.support.SendResult;

/**
 * Kafka 发送回调，兼容原生 Callback 与 Spring Kafka 的 CompletableFuture 回调。
 */
public class CallbackMsg implements Callback {

    private static final Logger log = LoggerFactory.getLogger(CallbackMsg.class);

    private final String msgId;

    public CallbackMsg() {
        this(null);
    }

    public CallbackMsg(String msgId) {
        this.msgId = msgId;
    }

    public void onSendComplete(SendResult<String, KafkaSendMsg> result, Throwable throwable) {
        if (throwable != null) {
            onCompletion(null, throwable instanceof Exception exception ? exception : new Exception(throwable));
            return;
        }
        onCompletion(result.getRecordMetadata(), null);
    }

    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception) {
        if (exception != null) {
            log.error("消息发送失败, msgId=[{}], error=[{}]", msgId, exception.getMessage());
            return;
        }
        log.info("消息发送成功, msgId=[{}], topic=[{}], partition=[{}], offset=[{}]",
                msgId, metadata.topic(), metadata.partition(), metadata.offset());
    }
}
