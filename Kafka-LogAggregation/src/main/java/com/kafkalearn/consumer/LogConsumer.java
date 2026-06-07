package com.kafkalearn.consumer;

import com.fasterxml.jackson.databind.DeserializationConfig;
import com.kafkalearn.entity.LogEntity;
import com.kafkalearn.msg.KafkaSendMsg;
import com.kafkalearn.repository.LogRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class LogConsumer {

    private final Logger log = LoggerFactory.getLogger(LogConsumer.class);

    private final LogRepository logRepository;

    public LogConsumer(LogRepository logRepository) {
        this.logRepository = logRepository;
    }

    @Transactional
    @KafkaListener(
            topics = "${app.kafka.produce-topic}",
            groupId = "${spring.kafka.consumer.group-id}"
    )
    public void consume(@Payload KafkaSendMsg message) {
        LogEntity entity = new LogEntity(message);
        logRepository.save(entity);
        log.info("日志已拉取并持久化, msgId=[{}], from=[{}], succ=[{}]",
                entity.getMsgId(), entity.getFrom(), entity.isSucc());
    }
}
