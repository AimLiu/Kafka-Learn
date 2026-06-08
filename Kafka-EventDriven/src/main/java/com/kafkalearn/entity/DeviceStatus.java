package com.kafkalearn.entity;

import com.kafkalearn.event.ActiveStatus;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.sql.Timestamp;
import java.util.UUID;

@Data
@NoArgsConstructor
@Entity
@Table(name = "device_status")
public class DeviceStatus {
    @Id
    @Column(name = "id", nullable = false)
    private UUID id;

    @Column(name = "device_id", nullable = false)
    private UUID deviceId;

    @Enumerated(EnumType.STRING)
    @Column(name = "alive", nullable = false)
    private ActiveStatus active;

    @Column(name = "last_connect_time", nullable = false)
    private Timestamp lastConnectTime;

}
