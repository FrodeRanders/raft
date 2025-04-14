package org.gautelis.raft.model;

public class Message {
    private String requestId = "unassigned";
    private String type;
    private Object payload;

    // Default constructor needed for Jackson
    public Message() {}

    public Message(String requestId, String type, Object payload) {
        this.requestId = requestId;
        this.type = type;
        this.payload = payload;
    }

    public String getRequestId() { return requestId; }
    public String getType() { return type; }
    public void setType(String type) { this.type = type; }
    public Object getPayload() { return payload; }
    public void setPayload(Object payload) { this.payload = payload; }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("Message{");
        sb.append("requestId='").append(requestId).append('\'');
        sb.append(", type='").append(type).append('\'');
        sb.append(", payload=").append(payload);
        sb.append('}');
        return sb.toString();
    }
}
