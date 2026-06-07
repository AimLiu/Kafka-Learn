package com.kafkalearn.demo.repository;

import com.kafkalearn.demo.domain.TaskStatus;
import com.kafkalearn.demo.entity.CallbackTaskEntity;
import jakarta.transaction.Transactional;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

public interface CallbackTaskRepository extends JpaRepository<CallbackTaskEntity, UUID> {

    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Transactional
    @Query("""
            update CallbackTaskEntity t
            set t.status = :targetStatus,
                t.updatedAt = :updatedAt
            where t.id = :taskId
              and t.status = :sourceStatus
            """)
    int claimStatus(@Param("taskId") UUID taskId,
                    @Param("sourceStatus") TaskStatus sourceStatus,
                    @Param("targetStatus") TaskStatus targetStatus,
                    @Param("updatedAt") LocalDateTime updatedAt);

    List<CallbackTaskEntity> findTop20ByStatusAndNextRetryTimeLessThanEqualOrderByNextRetryTimeAsc(
            TaskStatus status,
            LocalDateTime nextRetryTime
    );

    List<CallbackTaskEntity> findTop20ByStatusAndUpdatedAtLessThanEqualOrderByUpdatedAtAsc(
            TaskStatus status,
            LocalDateTime updatedAt
    );
}
