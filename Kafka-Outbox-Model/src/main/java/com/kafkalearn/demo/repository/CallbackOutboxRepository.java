package com.kafkalearn.demo.repository;

import com.kafkalearn.demo.domain.OutboxStatus;
import com.kafkalearn.demo.entity.CallbackOutboxEntity;
import org.springframework.data.jpa.repository.JpaRepository;

import java.time.LocalDateTime;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

public interface CallbackOutboxRepository extends JpaRepository<CallbackOutboxEntity, UUID> {

    List<CallbackOutboxEntity> findTop20ByStatusInAndNextAttemptTimeLessThanEqualOrderByCreatedAtAsc(
            Collection<OutboxStatus> statuses,
            LocalDateTime nextAttemptTime
    );
}
