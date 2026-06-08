package com.kafkalearn.repository;

import com.kafkalearn.entity.EsEvent;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

/**
 * 事件存储仓储。
 */
public interface EsEventRepository extends JpaRepository<EsEvent, UUID> {

    /**
     * 按聚合 ID 顺序查询全部事件。
     *
     * @param aggregateId 聚合 ID
     * @return 事件列表
     */
    List<EsEvent> findByAggregateIdOrderBySequenceAsc(UUID aggregateId);

    /**
     * 判断事件是否已存在（投影幂等）。
     *
     * @param eventId 事件 ID
     * @return 是否存在
     */
    boolean existsByEventId(UUID eventId);
}
