package org.gautelis.raft;

import org.gautelis.raft.storage.*;
import org.gautelis.raft.statemachine.*;
import org.gautelis.raft.transport.netty.*;
import org.gautelis.raft.serialization.ProtoMapper;

import io.netty.channel.embedded.EmbeddedChannel;
import org.gautelis.raft.protocol.VoteRequest;
import org.gautelis.raft.protocol.VoteResponse;
import org.gautelis.raft.proto.Envelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ClientResponseHandlerTest {
    private static final Logger log = LoggerFactory.getLogger(ClientResponseHandlerTest.class);

    static class CapturingMessageHandler implements MessageHandler {
        String correlationId;
        String type;
        byte[] payload;

        @Override
        public void handle(String correlationId, String type, byte[] payload, io.netty.channel.ChannelHandlerContext ctx) {
            this.correlationId = correlationId;
            this.type = type;
            this.payload = payload;
        }
    }

    @Test
    void voteResponseCompletesFuture() throws Exception {
        log.info("*** Testcase *** VoteResponse completes waiting future");

        CompletableFuture<VoteResponse> future = new CompletableFuture<>();
        Map<String, CompletableFuture<VoteResponse>> inFlight = new java.util.HashMap<>();
        inFlight.put("corr-1", future);

        ClientResponseHandler handler = new ClientResponseHandler(
                inFlight,
                new java.util.HashMap<>(),
                new java.util.HashMap<>(),
                new java.util.HashMap<>(),
                new java.util.HashMap<>(),
                new java.util.HashMap<>(),
                null
        );
        EmbeddedChannel channel = new EmbeddedChannel(handler);

        VoteRequest req = new VoteRequest(2, "A");
        VoteResponse resp = new VoteResponse(req, "B", true, 2);
        Envelope envelope = ProtoMapper.wrap(
                "corr-1",
                "VoteResponse",
                ProtoMapper.toProto(resp).toByteString()
        );

        channel.writeInbound(envelope);

        assertTrue(future.isDone());
        assertFalse(future.isCompletedExceptionally());
        assertEquals("B", future.get().getPeerId());
        assertNull(channel.readOutbound());
        channel.finishAndReleaseAll();
    }

    @Test
    void unknownTypeDelegatesToMessageHandler() throws Exception {
        log.info("*** Testcase *** Unknown type delegates to message handler");

        Map<String, CompletableFuture<VoteResponse>> inFlight = new java.util.HashMap<>();
        CapturingMessageHandler messageHandler = new CapturingMessageHandler();

        ClientResponseHandler handler = new ClientResponseHandler(
                inFlight,
                new java.util.HashMap<>(),
                new java.util.HashMap<>(),
                new java.util.HashMap<>(),
                new java.util.HashMap<>(),
                new java.util.HashMap<>(),
                messageHandler
        );
        EmbeddedChannel channel = new EmbeddedChannel(handler);

        byte[] payload = "ok".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        Envelope envelope = ProtoMapper.wrap("corr-2", "CustomType", payload);
        channel.writeInbound(envelope);

        assertEquals("corr-2", messageHandler.correlationId);
        assertEquals("CustomType", messageHandler.type);
        assertNotNull(messageHandler.payload);
        assertTrue(Arrays.equals(payload, messageHandler.payload));
        channel.finishAndReleaseAll();
    }

    @Test
    void missingFieldsAreIgnored() throws Exception {
        log.info("*** Testcase *** Testing incomplete envelope; WARN logs expected for missing type/correlationId");

        Map<String, CompletableFuture<VoteResponse>> inFlight = new java.util.HashMap<>();
        ClientResponseHandler handler = new ClientResponseHandler(
                inFlight,
                new java.util.HashMap<>(),
                new java.util.HashMap<>(),
                new java.util.HashMap<>(),
                new java.util.HashMap<>(),
                new java.util.HashMap<>(),
                null
        );
        EmbeddedChannel channel = new EmbeddedChannel(handler);

        Envelope noType = Envelope.newBuilder()
                .setCorrelationId("corr-3")
                .build();
        Envelope noCorrelation = Envelope.newBuilder()
                .setType("VoteResponse")
                .build();

        channel.writeInbound(noType);
        channel.writeInbound(noCorrelation);

        assertNull(channel.readOutbound());
        channel.finishAndReleaseAll();
    }
}
