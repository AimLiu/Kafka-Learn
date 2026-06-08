package com.kafkalearn.repository;

import com.kafkalearn.entity.DeviceStatus;
import com.kafkalearn.event.DeviceStatusChangedEvent;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

public interface DeviceStatusRepository extends JpaRepository<DeviceStatus, UUID> {
    DeviceStatus findByDeviceId(UUID deviceId);
}
