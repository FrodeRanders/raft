/*
 * Copyright (C) 2025-2026 Frode Randers
 * All rights reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gautelis.raft.model;

import com.fasterxml.uuid.Generators;

/**
 * Acts as both request and response pairs, and as a one-way message.
 */
public class Message {
    private String correlationId;
    private String type;
    private Object payload;

    // Default constructor needed for Jackson
    protected Message() {}

    public Message(String correlationId, String type, Object payload) {
        this.correlationId = correlationId;
        this.type = type;
        this.payload = payload;
    }

    public Message(String type, Object payload) {
        this(Generators.timeBasedEpochGenerator().generate().toString(), type, payload);
    }

    public String getCorrelationId() { return correlationId; }
    public String getType() { return type; }
    public void setType(String type) { this.type = type; }
    public Object getPayload() { return payload; }
    public void setPayload(Object payload) { this.payload = payload; }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("Message{");
        sb.append("correlationId='").append(correlationId).append('\'');
        sb.append(", type='").append(type).append('\'');
        sb.append(", payload=").append(payload);
        sb.append('}');
        return sb.toString();
    }
}
