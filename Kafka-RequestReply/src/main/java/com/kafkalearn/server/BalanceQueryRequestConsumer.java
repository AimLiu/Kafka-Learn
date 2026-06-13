package com.kafkalearn.server;

import com.kafkalearn.codec.BalanceQueryRequestCodec;
import com.kafkalearn.codec.BalanceQueryResponseCodec;
import com.kafkalearn.config.KafkaTopic;
import com.kafkalearn.message.BalanceQueryRequest;
import com.kafkalearn.message.BalanceQueryResponse;
import com.kafkalearn.message.KafkaHeaderNames;
import com.kafkalearn.message.ReplyStatus;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

/**
 * @author: Mafeifei
 * @email: owntnow@163.com
 * @Date: 2026/6/12 22:56
 * @Description:
 */

@Slf4j
@Component
@RequiredArgsConstructor
public class BalanceQueryRequestConsumer {

    private final BalanceQueryRequestCodec requestCodec;
    private final BalanceQueryResponseCodec responseCodec;
    private final InMemoryAccountBalanceStore balanceStore;
    private final KafkaTemplate<String, String> kafkaTemplate;

    //     private String requestGroupId = "balance-query-server-group";
    //    private String replyGroupId = "balance-query-client-group";
    @KafkaListener(
            topics = KafkaTopic.BALANCE_QUERY_REQUEST,
            groupId = "balance-query-server-group"
    )
    public void onRequest(ConsumerRecord<String, String> record) {
        String correlationId = headerValue(record, KafkaHeaderNames.CORRELATION_ID);
        // 取出header中携带的回复topic, 这里可以塞入不同的replyTopic，方便后续拓展
        String replyTo = headerValue(record, KafkaHeaderNames.REPLY_TO);
        // 接收到查询账单请求
        BalanceQueryRequest request = requestCodec.decode(record.value());

        log.info("Received balance query request, correlationId={}, accountId={}", correlationId, request.accountId());

        // 获取消息体中携带的延迟数
        long delayMs = parseSimulateDelay(record);
        if (delayMs > 0) {
            try {
                Thread.sleep(delayMs);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
        }

        // 模拟查询账单
        BalanceQueryResponse response = buildResponse(request);
        // 回复topic
        String replyTopic = (replyTo == null || replyTo.isBlank())
                ? KafkaTopic.BALANCE_QUERY_REPLY
                : replyTo;

        // 将查询的结果发布给reply消费, replyConsumer进行消费后，组装结果
        ProducerRecord<String, String> replyRecord = new ProducerRecord<>(replyTopic, responseCodec.encode(response));
        replyRecord.headers().add(new RecordHeader(KafkaHeaderNames.CORRELATION_ID, correlationId.getBytes(StandardCharsets.UTF_8)));

        kafkaTemplate.send(replyRecord);
        log.info("Sent balance query reply, correlationId={}, status={}", correlationId, response.status());
    }

    /**
     * 构建回复结果
     * @param request
     * @return 账单结果
     */
    private BalanceQueryResponse buildResponse(BalanceQueryRequest request) {
        Optional<BigDecimal> balance = balanceStore.findBalance(request.accountId());
        long now = System.currentTimeMillis();
        if (balance.isEmpty()) {
            return new BalanceQueryResponse(
                    request.accountId(), null, ReplyStatus.NOT_FOUND, "Account not found", now);
        }
        return new BalanceQueryResponse(
                request.accountId(), balance.get(), ReplyStatus.SUCCESS, null, now);
    }

    /**
     * 去除map结构的header中的value, 使用name作为key
     * @param record
     * @param name
     * @return value
     */
    private String headerValue(ConsumerRecord<String, String> record, String name) {
        var header = record.headers().lastHeader(name);
        if (header == null) {
            return null;
        }
        return new String(header.value(), StandardCharsets.UTF_8);
    }

    private long parseSimulateDelay(ConsumerRecord<String, String> record) {
        var header = record.headers().lastHeader("simulateDelayMs");
        if (header == null) {
            return 0L;
        }
        try {
            return Long.parseLong(new String(header.value(), StandardCharsets.UTF_8));
        } catch (NumberFormatException ex) {
            return 0L;
        }
    }
}