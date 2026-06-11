package com.kafkalearn.repository;

import com.kafkalearn.entity.AlertEventEntity;
import com.kafkalearn.entity.AlertPushLogEntity;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.UUID;

public interface AlertPushLogRepository extends JpaRepository<AlertPushLogEntity, UUID> {

    AlertPushLogEntity findByEventId(UUID eventId);
}
