package com.kafkalearn.codec;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafkalearn.message.BalanceQueryRequest;
import org.springframework.stereotype.Component;

/**
 * @author: Mafeifei
 * @email: owntnow@163.com
 * @Date: 2026/6/12 22:48
 * @Description:
 */


@Component
public class BalanceQueryRequestCodec {
    private final ObjectMapper objectMapper;

    public BalanceQueryRequestCodec(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public String encode(BalanceQueryRequest request) {
        try {
            return objectMapper.writeValueAsString(request);
        } catch (Exception ex) {
            throw new IllegalArgumentException("Failed to encode BalanceQueryRequest", ex);
        }
    }

    public BalanceQueryRequest decode(String json) {
        try {
            return objectMapper.readValue(json, BalanceQueryRequest.class);
        } catch (Exception ex) {
            throw new IllegalArgumentException("Failed to decode BalanceQueryRequest", ex);
        }
    }
}
