package com.kafkalearn.config;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;
import com.kafkalearn.serde.JsonSerdeFactory;

/**
 * Kafka Producer / Consumer 配置，供 Simulator、HTTP 注入与输出日志 Consumer 使用。
 *
 * <p>Streams Topology 的 Serde 由 {@link JsonSerdeFactory} 单独提供。
 */
@Slf4j
@Configuration
@EnableKafka
@RequiredArgsConstructor
public class KafkaAutoConfiguration {

    private final KafkaProperties kafkaProperties;

    /**
     * 创建 ProducerFactory，供 KafkaTemplate 发送输入 Topic 消息。
     *
     * @return String Key/Value 的 ProducerFactory
     */
    @Bean
    @ConditionalOnMissingBean
    public ProducerFactory<String, String> producerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        KafkaProperties.ProducerConfig producer = kafkaProperties.getProducer();
        props.put(ProducerConfig.ACKS_CONFIG, producer.getAcks());
        props.put(ProducerConfig.RETRIES_CONFIG, producer.getRetries());
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, producer.getBatchSize());
        props.put(ProducerConfig.LINGER_MS_CONFIG, producer.getLingerMs());
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, producer.getCompressionType());
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, producer.getRequestTimeoutMs());
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, producer.isEnableIdempotence());

        log.info("Kafka Producer 配置完成, bootstrap-servers={}", kafkaProperties.getBootstrapServers());
        return new DefaultKafkaProducerFactory<>(props);
    }

    /**
     * 创建 ConsumerFactory，供输出 Topic 日志 Consumer 使用。
     *
     * @return String Key/Value 的 ConsumerFactory
     */
    @Bean
    @ConditionalOnMissingBean
    public ConsumerFactory<String, String> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        KafkaProperties.ConsumerConfig consumer = kafkaProperties.getConsumer();
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, consumer.getAutoOffsetReset());
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, consumer.getMaxPollRecords());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, consumer.getEnableAutoCommit());

        log.info("Kafka Consumer 配置完成, auto-offset-reset={}", consumer.getAutoOffsetReset());
        return new DefaultKafkaConsumerFactory<>(props);
    }

    /**
     * KafkaTemplate，Simulator 与 HTTP 注入共用。
     *
     * @param producerFactory Producer 工厂
     * @return KafkaTemplate 实例
     */
    @Bean
    @ConditionalOnMissingBean
    public KafkaTemplate<String, String> kafkaTemplate(ProducerFactory<String, String> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }

    /**
     * @KafkaListener 容器工厂。
     *
     * @param consumerFactory Consumer 工厂
     * @return 并发 Listener 容器工厂
     */
    @Bean
    @ConditionalOnMissingBean(name = "kafkaListenerContainerFactory")
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(
            ConsumerFactory<String, String> consumerFactory) {
        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        return factory;
    }
}
