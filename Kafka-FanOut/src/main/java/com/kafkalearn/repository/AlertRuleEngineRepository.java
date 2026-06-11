package com.kafkalearn.repository;

import com.kafkalearn.entity.AlertPushLogEntity;
import com.kafkalearn.entity.AlertRuleEngineEntity;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.UUID;

public interface AlertRuleEngineRepository extends JpaRepository<AlertRuleEngineEntity, UUID> {

    AlertRuleEngineEntity findByEventId(UUID eventId);

}
