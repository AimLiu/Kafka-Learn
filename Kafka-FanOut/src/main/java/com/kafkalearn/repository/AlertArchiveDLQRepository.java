package com.kafkalearn.repository;

import com.kafkalearn.entity.AlertArchiveDLQEntity;
import com.kafkalearn.entity.AlertPushLogEntity;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.UUID;

public interface AlertArchiveDLQRepository extends JpaRepository<AlertArchiveDLQEntity, UUID> {
    AlertArchiveDLQEntity findByEventId(UUID eventId);
}
