package com.kafkalearn.client;

import com.kafkalearn.message.BalanceQueryResponse;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author: Mafeifei
 * @email: owntnow@163.com
 * @Date: 2026/6/12 22:50
 * @Description: 将等待回复的请求添加进 {@link ConcurrentHashMap} 中，当接收到回复，则将等待请求取出，并回复
 */


@Component
public class PendingReplyRegistry {

    private final ConcurrentHashMap<String, CompletableFuture<BalanceQueryResponse>> pending = new ConcurrentHashMap<>();

    public CompletableFuture<BalanceQueryResponse> register(String correlationId) {
        return pending.computeIfAbsent(correlationId, id -> new CompletableFuture<>());
    }

    /**
     * 将correlationId绑定的任务标记为完成
     * @param correlationId
     * @param response
     * @return 返回完成结果
     */
    public boolean complete(String correlationId, BalanceQueryResponse response) {
        CompletableFuture<BalanceQueryResponse> future = pending.remove(correlationId);
        if (future == null) {
            return false;
        }
        return future.complete(response);
    }

    public void remove(String correlationId) {
        CompletableFuture<BalanceQueryResponse> future = pending.remove(correlationId);
        if (future != null) {
            future.cancel(true);
        }
    }
}
