package org.gautelis.raft.model;

public class Message {
    private String type;
    private Object payload;

    // Default constructor needed for Jackson
    public Message() {}

    public Message(String type, Object payload) {
        this.type = type;
        this.payload = payload;
    }

    public String getType() { return type; }
    public void setType(String type) { this.type = type; }
    public Object getPayload() { return payload; }
    public void setPayload(Object payload) { this.payload = payload; }
}
