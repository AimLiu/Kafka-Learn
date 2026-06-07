package com.kafkalearn.producer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.kafkalearn.callback.CallbackMsg;
import com.kafkalearn.config.KafkaProperties;
import com.kafkalearn.msg.KafkaSendMsg;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import javax.print.attribute.standard.MediaSize;
import java.time.Instant;
import java.time.temporal.TemporalField;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.random.RandomGenerator;

@Service
public class SheduleProducer {

    private final Logger log = LoggerFactory.getLogger(SheduleProducer.class);
    private final KafkaMsgProducer kafkaMsgProducer;
    private final KafkaProperties properties;
    private final JsonNodeFactory jsonNodeFactory;

    public SheduleProducer(KafkaMsgProducer kafkaMsgProducer,
                           KafkaProperties properties) {
        this.kafkaMsgProducer = kafkaMsgProducer;
        this.properties = properties;
        this.jsonNodeFactory = JsonNodeFactory.instance;
    }

    @Scheduled(fixedDelayString = "${app.scheduler.send-delay-ms:5000}")
    public void produceLoop() {
        Instant instant = Instant.now();
        Map<String, JsonNode> map = new HashMap<>();
        Random generator = new Random();
        try {
            int nextInt = generator.nextInt(100);

            map.put("currentTime", new LongNode(instant.toEpochMilli()));
            map.put("from", nextInt <= 50 ? new TextNode("google") : new TextNode("baidu"));
            map.put("msg", nextInt <= 50 ? new TextNode("this is succ msg") : new TextNode("this is faild msg"));
            map.put("succ", nextInt <= 50 ? jsonNodeFactory.booleanNode(true) : jsonNodeFactory.booleanNode(false));
            JsonNode payload = new ObjectNode(jsonNodeFactory, map);
            KafkaSendMsg msg = new KafkaSendMsg(UUID.randomUUID().toString(), payload);

            kafkaMsgProducer.sendAsync(
                    properties.getProduceTopic(),
                    msg,
                    new CallbackMsg(msg.getMsgId())
            );
        } catch (Exception e) {
            log.error("定时发送消息异常: {}", e.getMessage(), e);
        }
    }
}
