package com.kafkalearn.config;

import com.kafkalearn.topology.IntrusionDetectionTopology;
import com.kafkalearn.topology.RoomTemperatureTopology;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.StreamsBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;

/**
 * 注册 Kafka Streams 两条 Topology 到同一 {@code application-id}。
 *
 * <p>须通过带 {@link StreamsBuilder} 参数的 {@link Autowired} 方法注册 DSL；
 * 单独声明 {@code Consumer<StreamsBuilder>} Bean 不会被 Spring Kafka 自动调用，会导致空 Topology 启动失败。
 */
@Configuration
@RequiredArgsConstructor
public class StreamsTopologyConfig {

    private final RoomTemperatureTopology roomTemperatureTopology;
    private final IntrusionDetectionTopology intrusionDetectionTopology;

    /**
     * Spring Kafka 创建 {@link StreamsBuilder} 后回调此方法，将两条流水线 DSL 注册进同一 Topology。
     *
     * @param builder StreamsBuilder 实例
     */
    @Autowired
    public void configureKafkaStreamsTopology(StreamsBuilder builder) {
        roomTemperatureTopology.build(builder);
        intrusionDetectionTopology.build(builder);
    }
}
