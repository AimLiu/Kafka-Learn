package com.kafkalearn.producer;

import ch.qos.logback.core.util.StringUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.kafkalearn.config.ProducerKafkaProperties;
import com.kafkalearn.msg.KafkaSendMsg;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@Service
public class ProduceSheduler {
    private final Logger log = LoggerFactory.getLogger(ProduceSheduler.class);
    private final KafkaMsgProducer kafkaMsgProducer;
    private final ProducerKafkaProperties properties;
    private final JsonNodeFactory jsonNodeFactory;

    public ProduceSheduler(KafkaMsgProducer kafkaMsgProducer,
                           ProducerKafkaProperties properties) {
        this.kafkaMsgProducer = kafkaMsgProducer;
        this.properties = properties;
        this.jsonNodeFactory = JsonNodeFactory.instance;
    }

    /**
     * 定时扫描待发布的 Outbox 记录，补偿 Kafka 投递失败场景。
     */
    @Scheduled(fixedDelayString = "${app.scheduler.send-delay-ms:5000}")
    public void produceLoop(){
        Instant instant = Instant.now();
        Map<String, JsonNode> map = new HashMap<>();
        try {
            map.put("currentTime", new IntNode(instant.getNano()));
            map.put("msgBody", new TextNode("current msg body is: test"));
            JsonNode payload = new ArrayNode(jsonNodeFactory)
                    .add(new ObjectNode(jsonNodeFactory, map));

            KafkaSendMsg msg = new KafkaSendMsg(UUID.randomUUID().toString(), payload);
            log.info("start send to topic [{}], kafka msg: msgId = [{}], payload = [{}]", properties.getProduceTopic(),msg.getMsgId(), payload);
            CompletableFuture<SendResult<String, KafkaSendMsg>> future = kafkaMsgProducer.sendMsg(properties.getProduceTopic(), msg);
            future.whenComplete((result, e) -> {
                log.info("curr msg send success, and the result :[{}]", result);
                if (e != null) {
                    log.error("curr msg send failure: [{}]", e.getMessage());
                }
            });
        }catch (Exception e){
            log.error(e.getMessage());
        }
    }
}
