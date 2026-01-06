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
package org.gautelis.raft;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.channel.embedded.EmbeddedChannel;
import org.gautelis.raft.model.Message;
import org.gautelis.raft.model.VoteRequest;
import org.gautelis.raft.model.VoteResponse;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ClientResponseHandlerTest {
    private final ObjectMapper mapper = new ObjectMapper();

    static class CapturingMessageHandler implements MessageHandler {
        String correlationId;
        String type;
        JsonNode payload;

        @Override
        public void handle(String correlationId, String type, JsonNode node, io.netty.channel.ChannelHandlerContext ctx) {
            this.correlationId = correlationId;
            this.type = type;
            this.payload = node;
        }
    }

    @Test
    void voteResponseCompletesFuture() throws Exception {
        CompletableFuture<VoteResponse> future = new CompletableFuture<>();
        Map<String, CompletableFuture<VoteResponse>> inFlight = new java.util.HashMap<>();
        inFlight.put("corr-1", future);

        ClientResponseHandler handler = new ClientResponseHandler(inFlight, null);
        EmbeddedChannel channel = new EmbeddedChannel(handler);

        VoteRequest req = new VoteRequest(2, "A");
        VoteResponse resp = new VoteResponse(req, "B", true, 2);
        Message msg = new Message("corr-1", "VoteResponse", resp);

        channel.writeInbound(mapper.valueToTree(msg));

        assertTrue(future.isDone());
        assertFalse(future.isCompletedExceptionally());
        assertEquals("B", future.get().getPeerId());
        assertNull(channel.readOutbound());
        channel.finishAndReleaseAll();
    }

    @Test
    void unknownTypeDelegatesToMessageHandler() throws Exception {
        Map<String, CompletableFuture<VoteResponse>> inFlight = new java.util.HashMap<>();
        CapturingMessageHandler messageHandler = new CapturingMessageHandler();

        ClientResponseHandler handler = new ClientResponseHandler(inFlight, messageHandler);
        EmbeddedChannel channel = new EmbeddedChannel(handler);

        Message msg = new Message("corr-2", "CustomType", Map.of("value", "ok"));
        channel.writeInbound(mapper.valueToTree(msg));

        assertEquals("corr-2", messageHandler.correlationId);
        assertEquals("CustomType", messageHandler.type);
        assertNotNull(messageHandler.payload);
        assertEquals("ok", messageHandler.payload.get("value").asText());
        channel.finishAndReleaseAll();
    }

    @Test
    void missingFieldsAreIgnored() throws Exception {
        Map<String, CompletableFuture<VoteResponse>> inFlight = new java.util.HashMap<>();
        ClientResponseHandler handler = new ClientResponseHandler(inFlight, null);
        EmbeddedChannel channel = new EmbeddedChannel(handler);

        JsonNode noType = mapper.readTree("{\"correlationId\":\"corr-3\",\"payload\":{}}");
        JsonNode noCorrelation = mapper.readTree("{\"type\":\"VoteResponse\",\"payload\":{}}");

        channel.writeInbound(noType);
        channel.writeInbound(noCorrelation);

        assertNull(channel.readOutbound());
        channel.finishAndReleaseAll();
    }
}
