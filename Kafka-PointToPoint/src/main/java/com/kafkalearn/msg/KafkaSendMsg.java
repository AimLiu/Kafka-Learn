package com.kafkalearn.msg;

import com.fasterxml.jackson.databind.JsonNode;

public class KafkaSendMsg {
    private String msgId;
    private JsonNode payload;

    public KafkaSendMsg(String msgId, JsonNode payload) {
        this.msgId = msgId;
        this.payload = payload;
    }

    public String getMsgId() {
        return msgId;
    }

    public void setMsgId(String msgId) {
        this.msgId = msgId;
    }

    public JsonNode getPayload() {
        return payload;
    }

    public void setPayload(JsonNode payload) {
        this.payload = payload;
    }

    @Override
    public String toString() {
        return "KafkaSendMsg{" +
                "msgId='" + msgId + '\'' +
                ", payload=" + payload +
                '}';
    }
}
