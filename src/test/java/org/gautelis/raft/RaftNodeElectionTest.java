package org.gautelis.raft;

import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.ImmediateEventExecutor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.gautelis.raft.model.*;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class RaftNodeElectionTest {
    private static final Logger log = LogManager.getLogger(RaftNodeElectionTest.class);

    static final class CountingRandom extends Random {
        private int count;
        private final double value;

        CountingRandom(double value) {
            this.value = value;
        }

        @Override
        public double nextDouble() {
            count++;
            return value;
        }

        int getCount() {
            return count;
        }
    }

    static final class MutableTime implements RaftNode.TimeSource {
        private long now;
        MutableTime(long startMillis) { this.now = startMillis; }
        @Override public long nowMillis() { return now; }
        void advance(long deltaMillis) { now += deltaMillis; }
        void set(long v) { now = v; }
    }

    /**
     * In-memory "network": a RaftClient implementation that .
     */
    static class QueuedRaftClient extends RaftClient {
        private final String selfId;
        private final Map<String, RaftNode> nodesById;
        private final Queue<Runnable> queue = new ArrayDeque<>();

        QueuedRaftClient(String selfId, Map<String, RaftNode> nodesById) {
            super("test", null);
            this.selfId = selfId;
            this.nodesById = nodesById;
        }

        @Override
        public Future<List<VoteResponse>> requestVoteFromAll(Collection<Peer> peers, VoteRequest voteReq) {
            // Return a promise that is completed later
            var promise = ImmediateEventExecutor.INSTANCE.<List<VoteResponse>>newPromise();

            queue.add(() -> {
                List<VoteResponse> out = new ArrayList<>();
                for (Peer p : peers) {
                    RaftNode n = nodesById.get(p.getId());
                    if (n != null) {
                        out.add(n.handleVoteRequest(voteReq));
                    }
                }
                promise.setSuccess(out);
            });

            return promise;
        }

        @Override
        public java.util.concurrent.CompletableFuture<AppendEntriesResponse> sendAppendEntries(Peer peer, AppendEntriesRequest req) {
            java.util.concurrent.CompletableFuture<AppendEntriesResponse> future = new java.util.concurrent.CompletableFuture<>();
            queue.add(() -> {
                RaftNode node = nodesById.get(peer.getId());
                if (node == null) {
                    future.complete(new AppendEntriesResponse(req.getTerm(), peer.getId(), false, -1));
                    return;
                }
                future.complete(node.handleAppendEntries(req));
            });
            return future;
        }

        @Override
        public java.util.concurrent.CompletableFuture<InstallSnapshotResponse> sendInstallSnapshot(Peer peer, InstallSnapshotRequest req) {
            java.util.concurrent.CompletableFuture<InstallSnapshotResponse> future = new java.util.concurrent.CompletableFuture<>();
            queue.add(() -> {
                RaftNode node = nodesById.get(peer.getId());
                if (node == null) {
                    future.complete(new InstallSnapshotResponse(req.getTerm(), peer.getId(), false, req.getLastIncludedIndex()));
                    return;
                }
                future.complete(node.handleInstallSnapshot(req));
            });
            return future;
        }

        void flush() {
            while (!queue.isEmpty()) {
                queue.poll().run();
            }
        }

        @Override public void shutdown() {}
    }

    /**
     * Peer factory (simplified)
     */
    static Peer peer(String id) {
        return new Peer(id, /* InetSocketAddress */ null);
    }

    @Test
    void staleHeartbeatDoesNotRefreshTimeout() {
        log.info("*** Testcase *** Stale heartbeat does not refresh timeout");

        MutableTime time = new MutableTime(10_000);

        Peer me = peer("A");
        Peer b = peer("B");

        // RaftClient unused in this test
        RaftNode n = new RaftNode(me, List.of(b), 1_000, null, new QueuedRaftClient("A", Map.of()), new InMemoryLogStore(), time, new Random(123));

        log.debug("Put node at term 5 with an old lastHeartbeat");
        n.handleAppendEntries(new AppendEntriesRequest(5, "B", 0, 0, 0, List.of()));
        long old = time.nowMillis() - 5_000;
        n.setLastHeartbeatMillisForTest(old);

        log.info("Receive stale heartbeat (term 4) -> must NOT refresh");
        n.handleAppendEntries(new AppendEntriesRequest(4, "B", 0, 0, 0, List.of()));
        assertEquals(old, n.getLastHeartbeatMillisForTest(), "stale heartbeat must not refresh lastHeartbeat");
        assertEquals(5, n.getTerm(), "stale heartbeat must not decrease term");
        log.info("Successful test!");
    }

    @Test
    void staleLogCandidateCannotWin() {
        log.info("*** Testcase *** Stale log candidate cannot win");
        MutableTime time = new MutableTime(0);

        Peer a = peer("A");
        Peer b = peer("B");

        InMemoryLogStore storeB = new InMemoryLogStore();
        storeB.append(List.of(
                new LogEntry(1, "B"),
                new LogEntry(2, "B")
        ));

        RaftNode nodeB = new RaftNode(b, List.of(a), 100, null, new QueuedRaftClient("B", Map.of()), storeB, time, new Random(1));

        // Candidate A has stale term and index -> reject.
        VoteRequest staleTerm = new VoteRequest(3, "A", 2, 1);
        VoteResponse r1 = nodeB.handleVoteRequest(staleTerm);
        assertFalse(r1.isVoteGranted());

        // Candidate A has up-to-date term but stale index -> reject.
        VoteRequest staleIndex = new VoteRequest(3, "A", 1, 2);
        VoteResponse r2 = nodeB.handleVoteRequest(staleIndex);
        assertFalse(r2.isVoteGranted());

        // Candidate A matches log -> grant.
        VoteRequest upToDate = new VoteRequest(3, "A", 2, 2);
        VoteResponse r3 = nodeB.handleVoteRequest(upToDate);
        assertTrue(r3.isVoteGranted());
    }

    @Test
    void voteGrantedOncePerTermUnlessSameCandidate() {
        log.info("*** Testcase *** Vote granted once per term unless same candidate");

        MutableTime time = new MutableTime(0);

        Peer a = peer("A");
        Peer b = peer("B");
        Peer c = peer("C");

        RaftNode nodeB = new RaftNode(b, List.of(a, c), 100, null, new QueuedRaftClient("B", Map.of()), new InMemoryLogStore(), time, new Random(1));

        VoteRequest reqA = new VoteRequest(1, "A");
        VoteResponse r1 = nodeB.handleVoteRequest(reqA);
        assertTrue(r1.isVoteGranted());

        VoteRequest reqC = new VoteRequest(1, "C");
        VoteResponse r2 = nodeB.handleVoteRequest(reqC);
        assertFalse(r2.isVoteGranted());

        // Same candidate in same term should be granted again.
        VoteResponse r3 = nodeB.handleVoteRequest(reqA);
        assertTrue(r3.isVoteGranted());
    }

    @Test
    void staleVoteResponsesAreIgnored() {
        log.info("*** Testcase *** Stale vote responses are ignored");

        MutableTime time = new MutableTime(0);

        Peer a = peer("A");
        Peer b = peer("B");

        RaftNode nodeA = new RaftNode(a, List.of(b), 100, null, new QueuedRaftClient("A", Map.of()), new InMemoryLogStore(), time, new Random(1));

        nodeA.setLastHeartbeatMillisForTest(0);
        time.set(10_000);
        nodeA.electionTickForTest();
        assertEquals(1, nodeA.getTerm());
        assertEquals(RaftNode.State.CANDIDATE, nodeA.getStateForTest());

        // Move to a newer term before handling old responses.
        nodeA.handleAppendEntries(new AppendEntriesRequest(2, "B", 0, 0, 0, List.of()));
        assertEquals(2, nodeA.getTerm());
        assertEquals(RaftNode.State.FOLLOWER, nodeA.getStateForTest());

        VoteRequest oldReq = new VoteRequest(1, "A");
        List<VoteResponse> oldResponses = List.of(new VoteResponse(oldReq, "B", true, 1));
        nodeA.handleVoteResponsesForTest(oldResponses, 1);

        assertEquals(2, nodeA.getTerm());
        assertEquals(RaftNode.State.FOLLOWER, nodeA.getStateForTest());
    }

    @Test
    void majorityRequiredToBecomeLeader() {
        log.info("*** Testcase *** Majority required to become leader");

        MutableTime time = new MutableTime(0);

        Peer a = peer("A");
        Peer b = peer("B");
        Peer c = peer("C");
        Peer d = peer("D");
        Peer e = peer("E");

        RaftNode nodeA = new RaftNode(a, List.of(b, c, d, e), 100, null, new QueuedRaftClient("A", Map.of()), new InMemoryLogStore(), time, new Random(1));

        nodeA.setLastHeartbeatMillisForTest(0);
        time.set(10_000);
        nodeA.electionTickForTest();
        assertEquals(1, nodeA.getTerm());
        assertEquals(RaftNode.State.CANDIDATE, nodeA.getStateForTest());

        VoteRequest req = new VoteRequest(1, "A");
        List<VoteResponse> notEnough = List.of(
                new VoteResponse(req, "B", true, 1),
                new VoteResponse(req, "C", false, 1),
                new VoteResponse(req, "D", false, 1),
                new VoteResponse(req, "E", false, 1)
        );
        nodeA.handleVoteResponsesForTest(notEnough, 1);
        assertEquals(RaftNode.State.CANDIDATE, nodeA.getStateForTest());

        List<VoteResponse> enough = List.of(
                new VoteResponse(req, "B", true, 1),
                new VoteResponse(req, "C", true, 1),
                new VoteResponse(req, "D", true, 1),
                new VoteResponse(req, "E", false, 1)
        );
        nodeA.handleVoteResponsesForTest(enough, 1);
        assertEquals(RaftNode.State.LEADER, nodeA.getStateForTest());
    }

    @Test
    void candidateStepsDownOnSameTermHeartbeat() {
        log.info("*** Testcase *** Candidate steps down on same term heartbeat");

        MutableTime time = new MutableTime(0);

        Peer a = peer("A");
        Peer b = peer("B");

        Map<String, RaftNode> nodes = new HashMap<>();

        QueuedRaftClient clientA = new QueuedRaftClient("A", nodes);
        QueuedRaftClient clientB = new QueuedRaftClient("B", nodes);

        RaftNode nodeA = new RaftNode(a, List.of(b), 100, null, clientA, new InMemoryLogStore(), time, new Random(1));
        RaftNode nodeB = new RaftNode(b, List.of(a), 100, null, clientB, new InMemoryLogStore(), time, new Random(2));

        nodes.put("A", nodeA);
        nodes.put("B", nodeB);

        log.info("Force timeout at A -> starts election; vote delivery is queued so state stays CANDIDATE");
        nodeA.setLastHeartbeatMillisForTest(0);
        time.set(10_000);
        nodeA.electionTickForTest();

        assertEquals(RaftNode.State.CANDIDATE, nodeA.getStateForTest());
        assertEquals(1, nodeA.getTerm());
        log.info("Successful test!");

        log.info("Same-term heartbeat should step candidate down immediately");
        // Using “heartbeat from B” as a stand-in for “some (other) leader exists”.
        // That leader would not have voted for A. This is a test of A's behaviour,
        // so this is sufficient to trigger the right behaviour in A.
        nodeA.handleAppendEntries(new AppendEntriesRequest(1, "B", 0, 0, 0, List.of()));
        assertEquals(RaftNode.State.FOLLOWER, nodeA.getStateForTest());
        log.info("Successful test!");

        log.info("Now deliver queued votes; A should NOT become leader after stepping down");
        clientA.flush();
        assertEquals(RaftNode.State.FOLLOWER, nodeA.getStateForTest());
        assertFalse(nodeA.isLeader());

        log.info("Successful test!");
    }

    @Test
    void higherTermInVoteResponsesForcesStepDownEvenIfVoteGranted() {
        log.info("*** Testcase *** Higher term in vote responses forces stepdown even if vote is granted");

        MutableTime time = new MutableTime(0);

        Peer a = peer("A");
        Peer b = peer("B");

        RaftNode nodeA = new RaftNode(a, List.of(b), 100, null, new QueuedRaftClient("A", Map.of()), new InMemoryLogStore(), time, new Random(1));

        log.info("Make A candidate at term 1");
        nodeA.setLastHeartbeatMillisForTest(0);
        time.set(10_000);
        nodeA.electionTickForTest();
        assertEquals(RaftNode.State.CANDIDATE, nodeA.getStateForTest());
        assertEquals(1, nodeA.getTerm());
        log.info("Successful test!");

        log.info("Make A yield at term 1 when seeing higher term vote response");
        VoteRequest req = new VoteRequest(1, "A");
        // Pathological but good for guarding logic: voteGranted=true but currentTerm=2
        List<VoteResponse> responses = List.of(new VoteResponse(req, "B", true, 2)); // B voting for A
        nodeA.handleVoteResponsesForTest(responses, 1);

        assertEquals(RaftNode.State.FOLLOWER, nodeA.getStateForTest());
        assertEquals(2, nodeA.getTerm());
        log.info("Successful test!");
    }

    @Test
    void higherTermInRejectedVoteResponseForcesStepDown() {
        log.info("*** Testcase *** Higher term in rejected vote response forces stepdown");

        MutableTime time = new MutableTime(0);

        Peer a = peer("A");
        Peer b = peer("B");

        RaftNode nodeA = new RaftNode(a, List.of(b), 100, null, new QueuedRaftClient("A", Map.of()), new InMemoryLogStore(), time, new Random(1));

        nodeA.setLastHeartbeatMillisForTest(0);
        time.set(10_000);
        nodeA.electionTickForTest();
        assertEquals(RaftNode.State.CANDIDATE, nodeA.getStateForTest());
        assertEquals(1, nodeA.getTerm());

        VoteRequest req = new VoteRequest(1, "A");
        List<VoteResponse> responses = List.of(new VoteResponse(req, "B", false, 2));
        nodeA.handleVoteResponsesForTest(responses, 1);

        assertEquals(RaftNode.State.FOLLOWER, nodeA.getStateForTest());
        assertEquals(2, nodeA.getTerm());
    }

    @Test
    void unknownPeerAppendEntriesIsRejected() {
        log.info("*** Testcase *** Unknown peer AppendEntries is rejected");

        MutableTime time = new MutableTime(0);

        Peer a = peer("A");
        Peer b = peer("B");

        RaftNode nodeA = new RaftNode(a, List.of(b), 100, null, new QueuedRaftClient("A", Map.of()), new InMemoryLogStore(), time, new Random(1));

        nodeA.setLastHeartbeatMillisForTest(0);
        time.set(10_000);
        nodeA.electionTickForTest();
        assertEquals(RaftNode.State.CANDIDATE, nodeA.getStateForTest());
        assertEquals(1, nodeA.getTerm());

        long before = nodeA.getLastHeartbeatMillisForTest();
        nodeA.handleAppendEntries(new AppendEntriesRequest(99, "X", 0, 0, 0, List.of()));

        assertEquals(RaftNode.State.CANDIDATE, nodeA.getStateForTest());
        assertEquals(1, nodeA.getTerm());
        assertEquals(before, nodeA.getLastHeartbeatMillisForTest());
    }

    @Test
    void selfAppendEntriesIsRejected() {
        log.info("*** Testcase *** Self-origin AppendEntries is rejected");

        MutableTime time = new MutableTime(0);
        Peer a = peer("A");
        Peer b = peer("B");

        RaftNode nodeA = new RaftNode(a, List.of(b), 100, null, new QueuedRaftClient("A", Map.of()), new InMemoryLogStore(), time, new Random(1));
        nodeA.setLastHeartbeatMillisForTest(0);
        time.set(10_000);
        nodeA.electionTickForTest();
        assertEquals(RaftNode.State.CANDIDATE, nodeA.getStateForTest());
        assertEquals(1, nodeA.getTerm());

        long before = nodeA.getLastHeartbeatMillisForTest();
        nodeA.handleAppendEntries(new AppendEntriesRequest(99, "A", 0, 0, 0, List.of()));

        assertEquals(RaftNode.State.CANDIDATE, nodeA.getStateForTest());
        assertEquals(1, nodeA.getTerm());
        assertEquals(before, nodeA.getLastHeartbeatMillisForTest());
    }

    @Test
    void electionTimeoutJitterIsSampledOncePerTimeoutWindow() {
        log.info("*** Testcase *** Election timeout jitter sampled once per timeout window");

        MutableTime time = new MutableTime(0);
        CountingRandom rng = new CountingRandom(0.5);

        Peer a = peer("A");
        Peer b = peer("B");
        RaftNode nodeA = new RaftNode(a, List.of(b), 100, null, new QueuedRaftClient("A", Map.of()), new InMemoryLogStore(), time, rng);

        nodeA.setLastHeartbeatMillisForTest(0);

        time.set(120);
        nodeA.electionTickForTest();
        assertEquals(RaftNode.State.FOLLOWER, nodeA.getStateForTest());
        assertEquals(2, rng.getCount(), "first timeout window should sample jitter once");

        time.set(130);
        nodeA.electionTickForTest();
        assertEquals(RaftNode.State.FOLLOWER, nodeA.getStateForTest());
        assertEquals(2, rng.getCount(), "subsequent checks in same window must not re-sample jitter");

        time.set(151);
        nodeA.electionTickForTest();
        assertEquals(RaftNode.State.CANDIDATE, nodeA.getStateForTest());
        assertEquals(4, rng.getCount(), "new election should resample for next timeout window");
    }

    @Test
    void threeNodeElectionEventuallyElectsSingleLeader() {
        log.info("*** Testcase *** Three nodes, election eventually elects single leader");

        MutableTime time = new MutableTime(0);

        Peer a = peer("A");
        Peer b = peer("B");
        Peer c = peer("C");

        Map<String, RaftNode> nodes = new HashMap<>();

        QueuedRaftClient clientA = new QueuedRaftClient("A", nodes);
        QueuedRaftClient clientB = new QueuedRaftClient("B", nodes);
        QueuedRaftClient clientC = new QueuedRaftClient("C", nodes);

        RaftNode nodeA = new RaftNode(a, List.of(b, c), 200, null, clientA, new InMemoryLogStore(), time, new Random(1));
        RaftNode nodeB = new RaftNode(b, List.of(a, c), 200, null, clientB, new InMemoryLogStore(), time, new Random(2));
        RaftNode nodeC = new RaftNode(c, List.of(a, b), 200, null, clientC, new InMemoryLogStore(), time, new Random(3));

        nodes.put("A", nodeA);
        nodes.put("B", nodeB);
        nodes.put("C", nodeC);

        // Start with "recent" heartbeats so nobody instantly times out at t=0
        nodeA.setLastHeartbeatMillisForTest(time.nowMillis());
        nodeB.setLastHeartbeatMillisForTest(time.nowMillis());
        nodeC.setLastHeartbeatMillisForTest(time.nowMillis());

        for (int i = 0; i < 2_000; i++) {
            time.advance(10);

            // Timeout checks (may enqueue vote RPCs)
            nodeA.electionTickForTest();
            nodeB.electionTickForTest();
            nodeC.electionTickForTest();

            // Deliver queued vote RPCs + responses
            clientA.flush();
            clientB.flush();
            clientC.flush();

            // Leader heartbeats (synchronous in this harness)
            nodeA.heartbeatTickForTest();
            nodeB.heartbeatTickForTest();
            nodeC.heartbeatTickForTest();

            log.info("Check that there is (eventually) a leader");
            long leaders = Stream.of(nodeA, nodeB, nodeC).filter(RaftNode::isLeader).count();
            if (leaders == 1) {
                // Ensure it stays stable for a few more ticks
                for (int j = 0; j < 50; j++) {
                    time.advance(10);

                    // Timeout checks (may enqueue vote RPCs)
                    nodeA.electionTickForTest();
                    nodeB.electionTickForTest();
                    nodeC.electionTickForTest();

                    // Deliver queued vote RPCs + responses
                    clientA.flush();
                    clientB.flush();
                    clientC.flush();

                    // Leader heartbeats (synchronous in this harness)
                    nodeA.heartbeatTickForTest();
                    nodeB.heartbeatTickForTest();
                    nodeC.heartbeatTickForTest();
                }
                assertEquals(1, Stream.of(nodeA, nodeB, nodeC).filter(RaftNode::isLeader).count());
                log.info("Successful test!");
                return;
            }
        }

        fail("No leader elected within the simulated time budget");
    }
}
