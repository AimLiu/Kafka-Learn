package com.kafkalearn.codec;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafkalearn.message.BalanceQueryResponse;
import org.springframework.stereotype.Component;

/**
 * @author: Mafeifei
 * @email: owntnow@163.com
 * @Date: 2026/6/12 22:49
 * @Description:
 */


@Component
public class BalanceQueryResponseCodec {
    private final ObjectMapper objectMapper;

    public BalanceQueryResponseCodec(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public String encode(BalanceQueryResponse response) {
        try {
            return objectMapper.writeValueAsString(response);
        } catch (Exception ex) {
            throw new IllegalArgumentException("Failed to encode BalanceQueryResponse", ex);
        }
    }

    public BalanceQueryResponse decode(String json) {
        try {
            return objectMapper.readValue(json, BalanceQueryResponse.class);
        } catch (Exception ex) {
            throw new IllegalArgumentException("Failed to decode BalanceQueryResponse", ex);
        }
    }
}
