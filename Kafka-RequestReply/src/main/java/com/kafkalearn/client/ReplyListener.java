package com.kafkalearn.client;

import com.kafkalearn.codec.BalanceQueryResponseCodec;
import com.kafkalearn.config.KafkaTopic;
import com.kafkalearn.message.KafkaHeaderNames;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

/**
 * @author: Mafeifei
 * @email: owntnow@163.com
 * @Date: 2026/6/13 11:42
 * @Description:
 */

@Slf4j
@Component
@RequiredArgsConstructor
public class ReplyListener {

    private final PendingReplyRegistry pendingReplyRegistry;
    private final BalanceQueryResponseCodec responseCodec;

    //     private String requestGroupId = "balance-query-server-group";
    //    private String replyGroupId = "balance-query-client-group";
    @KafkaListener(
            topics = KafkaTopic.BALANCE_QUERY_REPLY,
            groupId = "balance-query-client-group"
    )
    public void onReply(ConsumerRecord<String, String> record) {
        var header = record.headers().lastHeader(KafkaHeaderNames.CORRELATION_ID);
        if (header == null) {
            log.warn("Ignore reply without correlationId header");
            return;
        }
        String correlationId = new String(header.value(), StandardCharsets.UTF_8);
        var response = responseCodec.decode(record.value());
        // 实际的correlationId相应的结果在request消费者那就已经完成，到这里只是因为需要下游的消息接收者，来标记完成后的一些后续动作，比如组装其他信息等等
        // 我觉得这样设计主要是为了解耦
        boolean matched = pendingReplyRegistry.complete(correlationId, response);
        log.info("Reply received, correlationId={}, matched={}", correlationId, matched);
    }
}