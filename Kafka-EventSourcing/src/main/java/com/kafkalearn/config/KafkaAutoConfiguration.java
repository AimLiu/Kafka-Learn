package com.kafkalearn.config;


import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
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

@Slf4j
@Configuration
@EnableKafka
@RequiredArgsConstructor
// 这里如果不配置@EnableConfigurationProperties注解，则会走自定义的组件注册
public class KafkaAutoConfiguration {
    private final KafkaProperties kafkaProperties;

    /**
     * 配置 Producer Factory
     */
    @Bean
    @ConditionalOnMissingBean
    public ProducerFactory producerFactory(KafkaProperties kafkaProperties) {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        KafkaProperties.ProducerConfig producer = kafkaProperties.getProducer();
        // 生产者确认级别 - 0: 不等待任何确认 - 1: 等待 leader 确认 - all: 等待所有副本确认
        props.put(ProducerConfig.ACKS_CONFIG, producer.getAcks());
        // 重试次数
        props.put(ProducerConfig.RETRIES_CONFIG, producer.getRetries());
        // 批处理大小
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, producer.getBatchSize());
        // 等待事件
        props.put(ProducerConfig.LINGER_MS_CONFIG, producer.getLingerMs());
        // 压缩类型
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, producer.getCompressionType());
        // 请求超时时间（毫秒）
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, producer.getRequestTimeoutMs());

        log.info("Kafka Producer 配置完成, bootstrap-servers: {}, acks: {}",
                kafkaProperties.getBootstrapServers(), producer.getAcks());

        return new DefaultKafkaProducerFactory<>(props);
    }

    /**
     * 配置 Consumer Factory
     */
    @Bean
    @ConditionalOnMissingBean
    public ConsumerFactory consumerFactory(KafkaProperties kafkaProperties) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        KafkaProperties.ConsumerConfig consumer = kafkaProperties.getConsumer();
        // 消费者组 ID
        props.put(ConsumerConfig.GROUP_ID_CONFIG, consumer.getGroupId());
        // 自动提交偏移量
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, consumer.getAutoCommitIntervalMs());
        // 自动重置偏移量策略 - earliest: 从最早的消息开始 - latest: 从最新的消息开始 - none: 如果没有找到消费者组，抛出异常
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, consumer.getAutoOffsetReset());
        // 单次拉取的最大记录数
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, consumer.getMaxPollRecords());
        // 会话超时时间（毫秒）
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, consumer.getSessionTimeoutMs());
        // 心跳间隔（毫秒）
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, consumer.getHeartbeatIntervalMs());

        log.info("Kafka Consumer 配置完成, group-id: {}, auto-offset-reset: {}",
                consumer.getGroupId(), consumer.getAutoOffsetReset());

        return new DefaultKafkaConsumerFactory<>(props);
    }


    /**
     * 配置 KafkaTemplate
     */
    @Bean
    @ConditionalOnMissingBean
    public KafkaTemplate<String, String> kafkaTemplate(ProducerFactory<String, String> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }

    /**
     * 配置 KafkaListenerContainerFactory
     *
     * <p>Spring Kafka 的 {@code @KafkaListener} 注解依赖名为
     * {@code kafkaListenerContainerFactory} 的 Bean 来创建消费者容器。
     * 若缺少此 Bean，监听器将无法启动，导致无法消费消息。
     */
    @Bean
    @ConditionalOnMissingBean(name = "kafkaListenerContainerFactory")
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(ConsumerFactory<String, String> consumerFactory) {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        // ConcurrentKafkaListenerContainerFactory主要是帮@KafkaLIstener创建容器，使用consumerFactory进行消费者配置
        factory.setConsumerFactory(consumerFactory);
        log.info("Kafka ListenerContainerFactory 配置完成");
        return factory;
    }

    /**
     * 配置 AdminClient（用于主题管理）
     */
    @Bean
    @ConditionalOnMissingBean
    public AdminClient adminClient() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
        log.info("Kafka AdminClient 配置完成, bootstrap-servers: {}", kafkaProperties.getBootstrapServers());
        return AdminClient.create(props);
    }

}
