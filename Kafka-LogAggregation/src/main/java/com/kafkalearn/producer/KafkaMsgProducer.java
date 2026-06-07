package com.kafkalearn.producer;

import com.kafkalearn.callback.CallbackMsg;
import com.kafkalearn.config.KafkaProperties;
import com.kafkalearn.msg.KafkaSendMsg;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@Service
public class KafkaMsgProducer {

    private static final Logger log = LoggerFactory.getLogger(KafkaMsgProducer.class);

    private final KafkaTemplate<String, KafkaSendMsg> kafkaTemplate;
    private final KafkaProperties properties;

    public KafkaMsgProducer(KafkaTemplate<String, KafkaSendMsg> kafkaTemplate,
                            KafkaProperties properties) {
        this.kafkaTemplate = kafkaTemplate;
        this.properties = properties;
    }

    public CompletableFuture<SendResult<String, KafkaSendMsg>> sendAsync(String topic,
                                                                         KafkaSendMsg kafkaSendMsg,
                                                                         CallbackMsg callbackMsg) {
        log.info("异步发送消息, topic=[{}], key=[{}], msg=[{}]", topic, kafkaSendMsg.getMsgId(), kafkaSendMsg);
        CallbackMsg callback = resolveCallback(kafkaSendMsg, callbackMsg);
        CompletableFuture<SendResult<String, KafkaSendMsg>> future =
                kafkaTemplate.send(topic, kafkaSendMsg.getMsgId(), kafkaSendMsg);
        future.whenComplete(callback::onSendComplete);
        return future;
    }

    public SendResult<String, KafkaSendMsg> sendSync(String topic,
                                                     KafkaSendMsg kafkaSendMsg,
                                                     CallbackMsg callbackMsg) throws Exception {
        log.info("同步发送消息, topic=[{}], key=[{}], msg=[{}]", topic, kafkaSendMsg.getMsgId(), kafkaSendMsg);
        CallbackMsg callback = resolveCallback(kafkaSendMsg, callbackMsg);
        try {
            SendResult<String, KafkaSendMsg> result = kafkaTemplate
                    .send(topic, kafkaSendMsg.getMsgId(), kafkaSendMsg)
                    .get(properties.getSendTimeoutSeconds(), TimeUnit.SECONDS);
            callback.onSendComplete(result, null);
            return result;
        } catch (Exception ex) {
            callback.onSendComplete(null, ex);
            throw ex;
        }
    }

    private CallbackMsg resolveCallback(KafkaSendMsg kafkaSendMsg, CallbackMsg callbackMsg) {
        return callbackMsg != null ? callbackMsg : new CallbackMsg(kafkaSendMsg.getMsgId());
    }
}
