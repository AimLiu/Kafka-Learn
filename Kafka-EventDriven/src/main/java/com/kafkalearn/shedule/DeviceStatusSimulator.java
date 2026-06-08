package com.kafkalearn.shedule;

import com.kafkalearn.event.ActiveStatus;
import com.kafkalearn.event.DeviceStatusChangedEvent;
import com.kafkalearn.producer.EventPublisher;
import lombok.RequiredArgsConstructor;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.Random;
import java.util.UUID;

@RequiredArgsConstructor
@Service
public class DeviceStatusSimulator {

    private final EventPublisher eventPublisher;

    @Scheduled(fixedRate = 5000)
    public void simulate() {
        Random random = new Random();
        UUID[] deviceIds = new UUID[]{
                UUID.fromString("c542567a-dfb1-4af7-940a-a5cada4372b6"),
                UUID.fromString("26ad1ca1-d0d4-4dff-b609-b199069f4f0d"),
                UUID.fromString("cf935c51-5bbe-4743-95b9-db13e89b5006")};
        // the result is greater than or equal origin and less than bound
        int offset = random.nextInt(0,3);
        ActiveStatus active = random.nextInt(100) > 50 ? ActiveStatus.ACTIVE: ActiveStatus.DISACTIVE;
        DeviceStatusChangedEvent event = active==ActiveStatus.ACTIVE?
                DeviceStatusChangedEvent.online(deviceIds[offset]):
                DeviceStatusChangedEvent.offline(deviceIds[offset]);
        eventPublisher.publishSync(event);
    }
}
