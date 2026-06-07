package com.kafkalearn.demo.repository;

import com.kafkalearn.demo.entity.CallbackConsumeLogEntity;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

public interface CallbackConsumeLogRepository extends JpaRepository<CallbackConsumeLogEntity, UUID> {

    List<CallbackConsumeLogEntity> findByTask_IdOrderByCreatedAtAsc(UUID taskId);
}
