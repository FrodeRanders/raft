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

import org.gautelis.raft.storage.*;
import org.gautelis.raft.statemachine.*;
import org.gautelis.raft.transport.netty.*;
import org.gautelis.raft.serialization.ProtoMapper;

import org.gautelis.raft.protocol.AppendEntriesRequest;
import org.gautelis.raft.protocol.AppendEntriesResponse;
import org.gautelis.raft.protocol.InstallSnapshotRequest;
import org.gautelis.raft.protocol.InstallSnapshotResponse;
import org.gautelis.raft.protocol.LogEntry;
import org.gautelis.raft.protocol.Peer;
import org.gautelis.raft.protocol.VoteRequest;
import org.gautelis.raft.protocol.VoteResponse;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RaftSnapshotTransferTest {
    private static final Logger log = LoggerFactory.getLogger(RaftSnapshotTransferTest.class);

    static final class MutableTime implements RaftNode.TimeSource {
        private long now;
        MutableTime(long now) { this.now = now; }
        @Override
        public long nowMillis() { return now; }
        void set(long now) { this.now = now; }
    }

    static final class SnapshotAwareClient extends RaftClient {
        private final AtomicInteger appendCalls = new AtomicInteger();
        private final AtomicInteger snapshotCalls = new AtomicInteger();
        private final AtomicReference<InstallSnapshotRequest> lastSnapshotRequest = new AtomicReference<>();

        SnapshotAwareClient() {
            super("test", null);
        }

        @Override
        public io.netty.util.concurrent.Future<List<VoteResponse>> requestVoteFromAll(java.util.Collection<Peer> peers, VoteRequest req) {
            var promise = io.netty.util.concurrent.ImmediateEventExecutor.INSTANCE.<List<VoteResponse>>newPromise();
            java.util.ArrayList<VoteResponse> responses = new java.util.ArrayList<>();
            for (Peer peer : peers) {
                responses.add(new VoteResponse(req, peer.getId(), true, req.getTerm()));
            }
            promise.setSuccess(responses);
            return promise;
        }

        @Override
        public CompletableFuture<AppendEntriesResponse> sendAppendEntries(Peer peer, AppendEntriesRequest req) {
            appendCalls.incrementAndGet();
            // Force the leader to realize follower is behind snapshot.
            return CompletableFuture.completedFuture(new AppendEntriesResponse(req.getTerm(), peer.getId(), false, 1));
        }

        @Override
        public CompletableFuture<InstallSnapshotResponse> sendInstallSnapshot(Peer peer, InstallSnapshotRequest req) {
            snapshotCalls.incrementAndGet();
            lastSnapshotRequest.set(req);
            return CompletableFuture.completedFuture(new InstallSnapshotResponse(req.getTerm(), peer.getId(), true, req.getLastIncludedIndex()));
        }

        @Override
        public void shutdown() {
            // no-op for test
        }
    }

    @Test
    void leaderFallsBackToInstallSnapshotForLaggingFollower() {
        log.info("*** Testcase *** Snapshot fallback replication: verifies leader switches from AppendEntries to InstallSnapshot when follower is behind compacted prefix");
        Peer a = new Peer("A", null);
        Peer b = new Peer("B", null);
        MutableTime time = new MutableTime(0);
        SnapshotAwareClient client = new SnapshotAwareClient();

        InMemoryLogStore store = new InMemoryLogStore();
        store.append(List.of(
                new LogEntry(1, "A", "one".getBytes(java.nio.charset.StandardCharsets.UTF_8)),
                new LogEntry(1, "A", "two".getBytes(java.nio.charset.StandardCharsets.UTF_8))
        ));
        store.compactUpTo(1);

        RaftNode leader = new RaftNode(a, List.of(b), 100, null, client, store, time, new Random(1));

        leader.setLastHeartbeatMillisForTest(0);
        time.set(10_000);
        leader.electionTickForTest();
        VoteRequest req = new VoteRequest(1, "A");
        leader.handleVoteResponsesForTest(List.of(new VoteResponse(req, "B", true, 1)), 1);
        assertTrue(leader.isLeader());

        // First tick drives append failure and decreases nextIndex to snapshot boundary.
        leader.heartbeatTickForTest();
        assertEquals(1, client.appendCalls.get());

        // Second tick should send snapshot.
        leader.heartbeatTickForTest();
        assertEquals(1, client.snapshotCalls.get());
        InstallSnapshotRequest snapshotRequest = client.lastSnapshotRequest.get();
        assertNotNull(snapshotRequest);
        assertEquals(1, snapshotRequest.getLastIncludedIndex());
        assertEquals(1, snapshotRequest.getLastIncludedTerm());
    }
}
