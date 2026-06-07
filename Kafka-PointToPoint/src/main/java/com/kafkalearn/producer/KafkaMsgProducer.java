package com.kafkalearn.producer;

import com.kafkalearn.callback.ProducerCallbackMsg;
import com.kafkalearn.msg.KafkaSendMsg;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
public class KafkaMsgProducer {
    private Logger log = LoggerFactory.getLogger(KafkaMsgProducer.class);
    private final KafkaTemplate<String, KafkaSendMsg> kafkaTemplate;

    public KafkaMsgProducer(KafkaTemplate<String, KafkaSendMsg> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public CompletableFuture<SendResult<String, KafkaSendMsg>> sendMsg(String topic, KafkaSendMsg kafkaSendMsg) {
        // log.info("curr topic is [{}], and the msg is: [{}]", topic, kafkaSendMsg);
        /*
        kafka事务发送消息
        kafkaTemplate.executeInTransaction(new KafkaOperations.OperationsCallback<>() {
            @Override
            public Object doInOperations(KafkaOperations operations) {
                return kafkaTemplate.send(topic, kafkaSendMsg);
            }
        });
        */
        // 这里没有配置kafka的事务配置，所以这里的kafkaTemplate不允许事务事件的消息发送
        // System.out.println(kafkaTemplate.isAllowNonTransactional());
        // 这是不带key的发送，这样会导致每条消息都持续的发送给一个分区
        // CompletableFuture<SendResult<String, KafkaSendMsg>> result = kafkaTemplate.send(topic, kafkaSendMsg);
        // 这是带key的发送方式，这样就会触发分区发送（使用kafkaSendMsg.getMsgId()作为key）
            log.info("curr topic is [{}], key is [{}], msg is: [{}]", topic, kafkaSendMsg.getMsgId(), kafkaSendMsg);
            return kafkaTemplate.send(topic, kafkaSendMsg.getMsgId(), kafkaSendMsg);
    }
}
