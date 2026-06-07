package com.kafkalearn.demo.service;

import com.kafkalearn.demo.messaging.CallbackEventMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

@Service
public class CallbackConsumerService {

    private static final Logger log = LoggerFactory.getLogger(CallbackConsumerService.class);

    private final CallbackMessageProcessor callbackMessageProcessor;

    public CallbackConsumerService(CallbackMessageProcessor callbackMessageProcessor) {
        this.callbackMessageProcessor = callbackMessageProcessor;
    }

    @KafkaListener(topics = "${app.kafka.callback-topic}")
    public void consume(CallbackEventMessage message,
                        @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                        @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                        @Header(KafkaHeaders.OFFSET) long offset) {
        log.info("Received Kafka message, taskId={}, bizNo={}, topic={}, partition={}, offset={}, simulateMode={}",
                message.taskId(), message.bizNo(), topic, partition, offset, message.simulateMode());
        callbackMessageProcessor.processMessage(message, topic, partition, offset);
    }
}
