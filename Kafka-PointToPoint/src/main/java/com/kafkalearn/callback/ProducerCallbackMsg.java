package com.kafkalearn.callback;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ProducerCallbackMsg implements Callback {
    Logger log = LoggerFactory.getLogger(ProducerCallbackMsg.class);

    @Override
    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
        log.info("msg send success, and the record meta data is : [{}]", recordMetadata);
        if (e != null) {
            log.error(e.getMessage());
        }
    }
}
