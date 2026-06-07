package com.kafkalearn.consumer;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.kafkalearn.config.ProducerKafkaProperties;
import com.kafkalearn.producer.KafkaMsgProducer;
import com.kafkalearn.producer.ProduceSheduler;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import javax.swing.*;
import java.time.Duration;
import java.util.Collections;

@Service
public class MyKafkaConsumer {
    private final Logger log = LoggerFactory.getLogger(MyKafkaConsumer.class);

    private final ProducerKafkaProperties properties;
    private final JsonNodeFactory jsonNodeFactory;

    public MyKafkaConsumer(KafkaMsgProducer kafkaMsgProducer, ProducerKafkaProperties properties) {
        this.properties = properties;
        this.jsonNodeFactory = JsonNodeFactory.instance;
    }

    @KafkaListener(topics = "kafka-learn-producer" , groupId="producer-consumer-group-1")
    public void defaultConsumer(@Payload String message){
        log.info("producer-consumer-group-1-1 consumer msg =[{}] ,and the msg type is null", message);
    }

    @KafkaListener(topics = "kafka-learn-producer" , groupId="producer-consumer-group-1")
    public void defaultConsumer2(@Payload String message){
        log.info("producer-consumer-group-1-2 consumer msg =[{}] ,and the msg type is null", message);
    }
}
