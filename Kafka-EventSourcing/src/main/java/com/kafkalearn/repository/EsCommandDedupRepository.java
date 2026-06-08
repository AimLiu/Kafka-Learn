package com.kafkalearn.repository;

import com.kafkalearn.entity.EsCommandDedup;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.UUID;

/**
 * 命令幂等仓储。
 */
public interface EsCommandDedupRepository extends JpaRepository<EsCommandDedup, UUID> {
}
