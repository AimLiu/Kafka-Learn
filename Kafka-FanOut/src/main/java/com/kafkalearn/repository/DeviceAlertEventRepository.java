package com.kafkalearn.repository;

import com.kafkalearn.entity.AlertEventEntity;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.UUID;

public interface DeviceAlertEventRepository extends JpaRepository<AlertEventEntity, UUID> {

    AlertEventEntity findByEventId(UUID eventId);
}
