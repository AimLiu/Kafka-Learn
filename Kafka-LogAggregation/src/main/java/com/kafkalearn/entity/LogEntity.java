package com.kafkalearn.entity;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.kafkalearn.msg.KafkaSendMsg;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.sql.Timestamp;
import java.util.UUID;

@Entity
@Table(name = "aggregation_log")
public class LogEntity {

    @Id
    @Column(name = "msg_id", nullable = false)
    private UUID msgId;

    @Column(name = "log_time", nullable = false)
    private Timestamp logTime;

    @Column(name = "log_from", nullable = false, length = 64)
    private String from;

    @Column(name = "msg", nullable = false, length = 512)
    private String msg;

    @Column(name = "succ", nullable = false)
    private boolean succ;

    public LogEntity() {
    }

    public LogEntity(KafkaSendMsg msg) {
        this.setMsgId(UUID.fromString(msg.getMsgId()));
        ObjectNode payload = (ObjectNode) msg.getPayload();
        this.setLogTime(new Timestamp(payload.get("currentTime").asLong()));
        this.setMsg(payload.get("msg").asText());
        this.setFrom(payload.get("from").asText());
        this.setSucc(payload.get("succ").asBoolean());
    }

    public Timestamp getLogTime() {
        return logTime;
    }

    public void setLogTime(Timestamp logTime) {
        this.logTime = logTime;
    }

    public String getFrom() {
        return from;
    }

    public void setFrom(String from) {
        this.from = from;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public boolean isSucc() {
        return succ;
    }

    public void setSucc(boolean succ) {
        this.succ = succ;
    }

    public UUID getMsgId() {
        return msgId;
    }

    public void setMsgId(UUID msgId) {
        this.msgId = msgId;
    }
}
