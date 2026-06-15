package com.kafkalearn.topology;

import com.kafkalearn.accumulator.RoomTempAccumulator;
import com.kafkalearn.config.KafkaTopic;
import com.kafkalearn.config.StreamsAppProperties;
import com.kafkalearn.message.AlertType;
import com.kafkalearn.message.RoomTempStatsRecord;
import com.kafkalearn.message.TempThresholdAlert;
import com.kafkalearn.message.TemperatureReading;
import com.kafkalearn.messaging.EventJsonCodec;
import com.kafkalearn.serde.JsonSerdeFactory;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.kstream.Windowed;
import org.springframework.stereotype.Component;

import java.time.Duration;

/**
 * 流水线 1：温湿度遥测 → 按房间 1 分钟 Tumbling Window 聚合 → 统计输出 + 超温告警。
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class RoomTemperatureTopology {

    private static final double MIN_TEMPERATURE = -40.0;
    private static final double MAX_TEMPERATURE = 80.0;
    private static final String STATE_STORE_NAME = "room-temp-window-store";

    private final StreamsAppProperties streamsAppProperties;
    private final EventJsonCodec eventJsonCodec;

    /**
     * 将温度窗口 Topology 注册到 StreamsBuilder。
     *
     * @param builder StreamsBuilder 实例
     */
    public void build(StreamsBuilder builder) {
        /**
         * Consumed.with(Serdes.String(), Serdes.String()) 配置的是 Kafka Record 的 Key 和 Value（消息体）
         * ┌─────────────────────────────────────────────────┐
         * │  Record                                         │
         * │  ├─ Key      → 分区路由、Join/groupBy 常用      │
         * │  ├─ Value    → 业务 payload（你这里的 JSON）    │
         * │  ├─ Headers  → 可选元数据（本模块未使用）       │
         * │  ├─ Timestamp                                   │
         * │  └─ Partition / Offset                          │
         * └─────────────────────────────────────────────────┘
         */
        KStream<String, String> rawStream = builder.stream(
                KafkaTopic.DEVICE_TELEMETRY_TEMPERATURE,
                Consumed.with(Serdes.String(), Serdes.String())
        );

        // 对原始数据做过滤，如果是非法数据则直接丢弃，空数据也丢弃
        KStream<String, TemperatureReading> readings = rawStream
                .<TemperatureReading>mapValues(json -> eventJsonCodec.deserialize(json, TemperatureReading.class))
                .filter((key, reading) -> reading != null)
                .filter((key, reading) -> isValidTemperature(reading.temperatureCelsius()));

        var temperatureSerde = JsonSerdeFactory.jsonSerde(
                TemperatureReading.class, eventJsonCodec.getObjectMapper());
        var accumulatorSerde = JsonSerdeFactory.jsonSerde(
                RoomTempAccumulator.class, eventJsonCodec.getObjectMapper());

        // 定义时间窗口大小，传入的是分钟数（1分钟）
        Duration windowSize = Duration.ofMinutes(streamsAppProperties.getTempWindowMinutes());
        // 按照roomId分组，计算时间窗口，随后将温度进行聚合，计算平均温度以及
        KTable<Windowed<String>, RoomTempAccumulator> windowedStats = readings
                // TemperatureReading使用roomId做分组key
                .groupBy((key, reading) -> reading.roomId(),
                        Grouped.with(Serdes.String(), temperatureSerde))
                .windowedBy(TimeWindows.ofSizeWithNoGrace(windowSize))
                .aggregate(
                        RoomTempAccumulator::empty,
                        // 这里因为readings是KStream<String, TemperatureReading> 类型，所以这里的reading是TemperatureReading类型
                        (roomId, reading, accumulator) -> accumulator.add(reading),
                        // 在配置状态存哪儿（State Store 名字、类型），状态里的 Key/Value 用什么 Serde 序列化（写入本地 RocksDB + 备份 changelog Topic 时）
                        Materialized.<String, RoomTempAccumulator, WindowStore<Bytes, byte[]>>as(STATE_STORE_NAME)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(accumulatorSerde)
                );

        KStream<String, String> statsStream = windowedStats
                .toStream()
                .filter((windowedRoomId, accumulator) -> accumulator.getCount() > 0)
                .map((windowedRoomId, accumulator) -> toStatsKeyValue(windowedRoomId, accumulator));

        // 房间温度状态流式上报
        statsStream
                // 对每个kStream进行一个操作（比如日志），该函数不会对流进行改动操作
                .peek((roomId, json) -> log.info("[STATS-TOPO] room={} payload={}", roomId, json))
                // 将内容传输到指定的topic中
                .to(KafkaTopic.STREAM_ROOM_TEMP_STATS, Produced.with(Serdes.String(), Serdes.String()));

        double threshold = streamsAppProperties.getTempThresholdCelsius();
        // 发送房间温度预警的流消息
        windowedStats
                .toStream()
                .filter((windowedRoomId, accumulator) -> accumulator.getCount() > 0 && accumulator.avg() > threshold)
                .map((windowedRoomId, accumulator) -> toAlertKeyValue(windowedRoomId, accumulator, threshold))
                .peek((roomId, json) -> log.info("[TEMP-ALERT-TOPO] room={} payload={}", roomId, json))
                .to(KafkaTopic.STREAM_TEMP_THRESHOLD_ALERT, Produced.with(Serdes.String(), Serdes.String()));
    }

    /**
     * 判断温度是否合法
     * @param temperature 档期那温度
     * @return 是否合法结果
     */
    private boolean isValidTemperature(Double temperature) {
        return temperature != null && temperature >= MIN_TEMPERATURE && temperature <= MAX_TEMPERATURE;
    }

    /**
     * 将accumulator转换为roomId->encod(accumulator)的类型
     * @param windowedRoomId
     * @param accumulator
     * @return
     */
    private KeyValue<String, String> toStatsKeyValue(Windowed<String> windowedRoomId, RoomTempAccumulator accumulator) {
        RoomTempStatsRecord record = new RoomTempStatsRecord(
                windowedRoomId.key(),
                accumulator.getHomeId(),
                windowedRoomId.window().start(),
                windowedRoomId.window().end(),
                accumulator.avg(),
                accumulator.getMax(),
                accumulator.getCount()
        );
        return KeyValue.pair(record.roomId(), eventJsonCodec.serialize(record));
    }

    private KeyValue<String, String> toAlertKeyValue(
            Windowed<String> windowedRoomId,
            RoomTempAccumulator accumulator,
            double threshold) {
        TempThresholdAlert alert = new TempThresholdAlert(
                windowedRoomId.key(),
                accumulator.getHomeId(),
                accumulator.avg(),
                threshold,
                windowedRoomId.window().start(),
                windowedRoomId.window().end(),
                AlertType.TEMP_HIGH,
                windowedRoomId.window().end()
        );
        return KeyValue.pair(alert.roomId(), eventJsonCodec.serialize(alert));
    }
}
