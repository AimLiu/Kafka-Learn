package com.kafkalearn.repository;

import com.kafkalearn.entity.EsAccountBalanceView;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.UUID;

/**
 * 余额读模型仓储。
 */
public interface EsAccountBalanceViewRepository extends JpaRepository<EsAccountBalanceView, UUID> {
}
