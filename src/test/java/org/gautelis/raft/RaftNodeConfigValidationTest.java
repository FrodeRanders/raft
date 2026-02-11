package org.gautelis.raft;

import org.gautelis.raft.storage.*;
import org.gautelis.raft.statemachine.*;
import org.gautelis.raft.transport.netty.*;
import org.gautelis.raft.serialization.ProtoMapper;

import org.gautelis.raft.protocol.Peer;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertThrows;

class RaftNodeConfigValidationTest {
    private static final Logger log = LoggerFactory.getLogger(RaftNodeConfigValidationTest.class);

    static class NoopRaftClient extends RaftClient {
        NoopRaftClient() {
            super("test", null);
        }

        @Override
        public void shutdown() {
            // no-op for tests
        }
    }

    @Test
    void rejectsDuplicatePeerIdWithConflictingAddress() {
        log.info("*** Testcase *** Membership conflict detection: verifies duplicate peer ids with different addresses are rejected at node construction");
        Peer me = new Peer("A", new InetSocketAddress("127.0.0.1", 10080));
        Peer b1 = new Peer("B", new InetSocketAddress("127.0.0.1", 10081));
        Peer b2 = new Peer("B", new InetSocketAddress("127.0.0.1", 10082));

        assertThrows(IllegalArgumentException.class, () -> new RaftNode(
                me,
                List.of(b1, b2),
                100,
                null,
                new NoopRaftClient(),
                new InMemoryLogStore(),
                new InMemoryPersistentStateStore(),
                System::currentTimeMillis,
                new Random(1)
        ));
    }

    @Test
    void rejectsSelfPeerWithConflictingAddress() {
        log.info("*** Testcase *** Self identity conflict detection: verifies self id duplicated with different address is rejected");
        Peer me = new Peer("A", new InetSocketAddress("127.0.0.1", 10080));
        Peer duplicateSelf = new Peer("A", new InetSocketAddress("127.0.0.1", 10081));

        assertThrows(IllegalArgumentException.class, () -> new RaftNode(
                me,
                List.of(duplicateSelf),
                100,
                null,
                new NoopRaftClient(),
                new InMemoryLogStore(),
                new InMemoryPersistentStateStore(),
                System::currentTimeMillis,
                new Random(1)
        ));
    }
}
