package com.kafkalearn.repository;

import com.kafkalearn.entity.EsAccount;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.UUID;

/**
 * 账户聚合根元数据仓储。
 */
public interface EsAccountRepository extends JpaRepository<EsAccount, UUID> {

    /**
     * 使用乐观锁递增账户事件版本。
     *
     * @param accountId       账户 ID
     * @param expectedVersion 期望的当前版本
     * @return 受影响行数，0 表示并发冲突
     */
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("""
            update EsAccount a
               set a.currentVersion = a.currentVersion + 1,
                   a.updatedAt = CURRENT_TIMESTAMP
             where a.accountId = :accountId
               and a.currentVersion = :expectedVersion
            """)
    int incrementVersion(@Param("accountId") UUID accountId, @Param("expectedVersion") long expectedVersion);

    /**
     * 关户时更新状态与版本。
     *
     * @param accountId       账户 ID
     * @param expectedVersion 期望版本
     * @return 受影响行数
     */
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("""
            update EsAccount a
               set a.status = com.kafkalearn.domain.AccountStatus.CLOSED,
                   a.currentVersion = a.currentVersion + 1,
                   a.updatedAt = CURRENT_TIMESTAMP
             where a.accountId = :accountId
               and a.currentVersion = :expectedVersion
               and a.status = com.kafkalearn.domain.AccountStatus.ACTIVE
            """)
    int closeAccount(@Param("accountId") UUID accountId, @Param("expectedVersion") long expectedVersion);
}
