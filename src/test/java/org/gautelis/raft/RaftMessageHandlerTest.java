package org.gautelis.raft;

import org.gautelis.raft.storage.*;
import org.gautelis.raft.statemachine.*;
import org.gautelis.raft.transport.netty.*;
import org.gautelis.raft.serialization.ProtoMapper;

import io.netty.channel.embedded.EmbeddedChannel;
import org.gautelis.raft.protocol.AppendEntriesRequest;
import org.gautelis.raft.protocol.AppendEntriesResponse;
import org.gautelis.raft.protocol.Peer;
import org.gautelis.raft.protocol.VoteRequest;
import org.gautelis.raft.protocol.VoteResponse;
import org.gautelis.raft.proto.Envelope;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RaftMessageHandlerTest {
    private static final Logger log = LoggerFactory.getLogger(RaftMessageHandlerTest.class);

    static class NoopRaftClient extends RaftClient {
        NoopRaftClient() {
            super("test", null);
        }
    }

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
    void voteRequestProducesVoteResponse() throws Exception {
        log.info("*** Testcase *** VoteRequest yields VoteResponse");

        Peer a = new Peer("A", null);
        Peer b = new Peer("B", null);
        RaftNode nodeB = new RaftNode(b, List.of(a), 100, null, new NoopRaftClient(), new InMemoryLogStore());

        EmbeddedChannel channel = new EmbeddedChannel(new RaftMessageHandler(nodeB));
        VoteRequest request = new VoteRequest(1, "A");
        Envelope envelope = ProtoMapper.wrap(
                "corr-1",
                "VoteRequest",
                ProtoMapper.toProto(request).toByteString()
        );
        channel.writeInbound(envelope);

        Envelope outbound = channel.readOutbound();
        assertNotNull(outbound);
        assertEquals("corr-1", outbound.getCorrelationId());
        assertEquals("VoteResponse", outbound.getType());

        var responseProto = ProtoMapper.parseVoteResponse(outbound.getPayload().toByteArray());
        assertTrue(responseProto.isPresent());
        VoteResponse response = ProtoMapper.fromProto(responseProto.get());
        assertEquals("B", response.getPeerId());
        assertEquals(1, response.getTerm());
        assertTrue(response.isVoteGranted());
        assertEquals(1, response.getCurrentTerm());
        channel.finishAndReleaseAll();
    }

    @Test
    void appendEntriesHeartbeatWritesResponseAndUpdatesTerm() throws Exception {
        log.info("*** Testcase *** Empty AppendEntries heartbeat updates term and returns AppendEntriesResponse");

        Peer a = new Peer("A", null);
        Peer b = new Peer("B", null);
        RaftNode nodeB = new RaftNode(b, List.of(a), 100, null, new NoopRaftClient(), new InMemoryLogStore());

        EmbeddedChannel channel = new EmbeddedChannel(new RaftMessageHandler(nodeB));
        AppendEntriesRequest heartbeat = new AppendEntriesRequest(2, "A", 0, 0, 0, List.of());
        Envelope envelope = ProtoMapper.wrap(
                "corr-2",
                "AppendEntriesRequest",
                ProtoMapper.toProto(heartbeat).toByteString()
        );
        channel.writeInbound(envelope);

        Envelope outbound = channel.readOutbound();
        assertNotNull(outbound);
        assertEquals("corr-2", outbound.getCorrelationId());
        assertEquals("AppendEntriesResponse", outbound.getType());
        var responseProto = ProtoMapper.parseAppendEntriesResponse(outbound.getPayload().toByteArray());
        assertTrue(responseProto.isPresent());
        AppendEntriesResponse response = ProtoMapper.fromProto(responseProto.get());
        assertTrue(response.isSuccess());
        assertEquals(2, nodeB.getTerm());
        assertEquals(RaftNode.State.FOLLOWER, nodeB.getStateForTest());
        channel.finishAndReleaseAll();
    }

    @Test
    void unknownTypeDelegatesToMessageHandler() throws Exception {
        log.info("*** Testcase *** Unknown type delegates to message handler");

        Peer a = new Peer("A", null);
        Peer b = new Peer("B", null);
        CapturingMessageHandler handler = new CapturingMessageHandler();
        RaftNode nodeB = new RaftNode(b, List.of(a), 100, handler, new NoopRaftClient(), new InMemoryLogStore());

        EmbeddedChannel channel = new EmbeddedChannel(new RaftMessageHandler(nodeB));
        byte[] payload = "hello".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        Envelope envelope = ProtoMapper.wrap("corr-3", "CustomType", payload);
        channel.writeInbound(envelope);

        assertEquals("corr-3", handler.correlationId);
        assertEquals("CustomType", handler.type);
        assertNotNull(handler.payload);
        assertTrue(Arrays.equals(payload, handler.payload));
        assertNull(channel.readOutbound());
        channel.finishAndReleaseAll();
    }
}
