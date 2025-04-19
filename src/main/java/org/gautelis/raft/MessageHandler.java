package org.gautelis.raft;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;

public interface MessageHandler {
    void handle(String correlationId, String type, JsonNode node) throws JsonProcessingException;
}
