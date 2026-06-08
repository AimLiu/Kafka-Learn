package com.kafkalearn.repository;

import com.kafkalearn.entity.EsAccountTransactionView;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

/**
 * 流水读模型仓储。
 */
public interface EsAccountTransactionViewRepository extends JpaRepository<EsAccountTransactionView, UUID> {

    /**
     * 按账户 ID 查询流水，按发生时间倒序。
     *
     * @param accountId 账户 ID
     * @return 流水列表
     */
    List<EsAccountTransactionView> findByAccountIdOrderByOccurredAtDesc(UUID accountId);

    /**
     * 判断流水是否已投影（幂等）。
     *
     * @param eventId 事件 ID
     * @return 是否已存在
     */
    boolean existsByEventId(UUID eventId);
}
