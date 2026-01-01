package org.gautelis.raft;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.buffer.ByteBuf;
import io.netty.channel.embedded.EmbeddedChannel;
import org.gautelis.raft.model.Heartbeat;
import org.gautelis.raft.model.Message;
import org.gautelis.raft.model.Peer;
import org.gautelis.raft.model.VoteRequest;
import org.gautelis.raft.model.VoteResponse;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RaftMessageHandlerTest {
    private final ObjectMapper mapper = new ObjectMapper();

    static class NoopRaftClient extends RaftClient {
        NoopRaftClient() {
            super(null);
        }
    }

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
    void voteRequestProducesVoteResponse() throws Exception {
        Peer a = new Peer("A", null);
        Peer b = new Peer("B", null);
        RaftNode nodeB = new RaftNode(b, List.of(a), 100, null, new NoopRaftClient(), new InMemoryLogStore());

        EmbeddedChannel channel = new EmbeddedChannel(new RaftMessageHandler(nodeB));
        Message msg = new Message("corr-1", "VoteRequest", new VoteRequest(1, "A"));
        channel.writeInbound(mapper.valueToTree(msg));

        ByteBuf outbound = channel.readOutbound();
        assertNotNull(outbound);
        try {
            String json = outbound.toString(StandardCharsets.UTF_8);
            JsonNode out = mapper.readTree(json);

            assertEquals("corr-1", out.get("correlationId").asText());
            assertEquals("VoteResponse", out.get("type").asText());

            VoteResponse response = mapper.treeToValue(out.get("payload"), VoteResponse.class);
            assertEquals("B", response.getPeerId());
            assertEquals(1, response.getTerm());
            assertTrue(response.isVoteGranted());
            assertEquals(1, response.getCurrentTerm());
        } finally {
            outbound.release();
            channel.finishAndReleaseAll();
        }
    }

    @Test
    void heartbeatDoesNotWriteResponseAndUpdatesTerm() throws Exception {
        Peer a = new Peer("A", null);
        Peer b = new Peer("B", null);
        RaftNode nodeB = new RaftNode(b, List.of(a), 100, null, new NoopRaftClient(), new InMemoryLogStore());

        EmbeddedChannel channel = new EmbeddedChannel(new RaftMessageHandler(nodeB));
        Message msg = new Message("corr-2", "Heartbeat", new Heartbeat(2, "A"));
        channel.writeInbound(mapper.valueToTree(msg));

        ByteBuf outbound = channel.readOutbound();
        assertNull(outbound);
        assertEquals(2, nodeB.getTerm());
        assertEquals(RaftNode.State.FOLLOWER, nodeB.getStateForTest());
        channel.finishAndReleaseAll();
    }

    @Test
    void unknownTypeDelegatesToMessageHandler() throws Exception {
        Peer a = new Peer("A", null);
        Peer b = new Peer("B", null);
        CapturingMessageHandler handler = new CapturingMessageHandler();
        RaftNode nodeB = new RaftNode(b, List.of(a), 100, handler, new NoopRaftClient(), new InMemoryLogStore());

        EmbeddedChannel channel = new EmbeddedChannel(new RaftMessageHandler(nodeB));
        Message msg = new Message("corr-3", "CustomType", Map.of("message", "hello"));
        channel.writeInbound(mapper.valueToTree(msg));

        assertEquals("corr-3", handler.correlationId);
        assertEquals("CustomType", handler.type);
        assertNotNull(handler.payload);
        assertEquals("hello", handler.payload.get("message").asText());
        assertNull(channel.readOutbound());
        channel.finishAndReleaseAll();
    }
}
