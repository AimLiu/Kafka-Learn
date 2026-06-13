package com.kafkalearn.client;

import com.kafkalearn.codec.BalanceQueryRequestCodec;
import com.kafkalearn.config.KafkaTopic;
import com.kafkalearn.exception.ReplyTimeoutException;
import com.kafkalearn.message.BalanceQueryRequest;
import com.kafkalearn.message.BalanceQueryResponse;
import com.kafkalearn.message.BalanceQueryResult;
import com.kafkalearn.message.KafkaHeaderNames;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @author: Mafeifei
 * @email: owntnow@163.com
 * @Date: 2026/6/13 11:43
 * @Description:
 */

@Slf4j
@Service
@RequiredArgsConstructor
public class BalanceQueryClient {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final BalanceQueryRequestCodec requestCodec;
    private final PendingReplyRegistry pendingReplyRegistry;

    public BalanceQueryResult query(UUID accountId, long simulateDelayMs) {
        String correlationId = UUID.randomUUID().toString();
        long startedAt = System.currentTimeMillis();

        BalanceQueryRequest request = new BalanceQueryRequest(accountId, startedAt);
        // 将后续包裹结果的future进行返回
        CompletableFuture<BalanceQueryResponse> future = pendingReplyRegistry.register(correlationId);

        //构建查询结果
        ProducerRecord<String, String> record = new ProducerRecord<>(KafkaTopic.BALANCE_QUERY_REQUEST, accountId.toString(), requestCodec.encode(request));
        record.headers().add(new RecordHeader(KafkaHeaderNames.CORRELATION_ID, correlationId.getBytes(StandardCharsets.UTF_8)));
        // 放入回复topic
        record.headers().add(new RecordHeader(KafkaHeaderNames.REPLY_TO, KafkaTopic.BALANCE_QUERY_REPLY.getBytes(StandardCharsets.UTF_8)));
        // 如果有延迟尝试，则加入延迟信息
        if (simulateDelayMs > 0) {
            record.headers().add(new RecordHeader("simulateDelayMs", Long.toString(simulateDelayMs).getBytes(StandardCharsets.UTF_8)));
        }

        kafkaTemplate.send(record);
        log.info("Balance query sent, correlationId={}, accountId={}", correlationId, accountId);

        try {
            BalanceQueryResponse response = future.get(3000, TimeUnit.MILLISECONDS);
            long elapsedMs = System.currentTimeMillis() - startedAt;
            return new BalanceQueryResult(correlationId, response, elapsedMs);
        } catch (Exception ex) {
            pendingReplyRegistry.remove(correlationId);
            if (ex instanceof TimeoutException || ex.getCause() instanceof TimeoutException) {
                throw new ReplyTimeoutException(correlationId, 3000);
            }
            throw new IllegalStateException("Balance query failed, correlationId=" + correlationId, ex);
        }
    }

}