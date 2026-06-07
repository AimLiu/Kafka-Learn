package com.kafkalearn.msg;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;

/**
 * @author: Mafeifei
 * @email: owntnow@163.com
 * @Date: 2026/6/7 16:23
 * @Description:
 */

public class KafkaSendMsg {
    private String msgId;
    private JsonNode payload;

    /**
     * 需要使用JsonCreator注解，否则无法进行解码
     * @param msgId
     * @param payload
     */
    @JsonCreator
    public KafkaSendMsg(@JsonProperty("msgId")String msgId, @JsonProperty("payload") JsonNode payload) {
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
