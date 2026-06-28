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
package org.gautelis.raft.app.compensator;

import org.gautelis.raft.ClusterConfiguration;
import org.gautelis.raft.MembershipChangeListener;
import org.gautelis.raft.RaftNode;
import org.gautelis.raft.bootstrap.AdapterSpec;
import org.gautelis.raft.bootstrap.BasicAdapter;
import org.gautelis.raft.bootstrap.RuntimeConfiguration;
import org.gautelis.raft.protocol.ClientCommandRequest;
import org.gautelis.raft.protocol.Peer;
import org.gautelis.raft.statemachine.SnapshotStateMachine;
import org.gautelis.raft.transport.RaftTransportFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Adapter for the compensator work-partitioning demo.
 *
 * <p>Key design for work partitioning:</p>
 * <ol>
 *   <li>The cluster membership (voting members) determines how many compensators exist.</li>
 *   <li>Each compensator's position is its index in the sorted voting member list.</li>
 *   <li>Work is partitioned: a compensation record belongs to the compensator where
 *       {@code hash(recordId) % totalCompensators == myPosition}.</li>
 *   <li>{@link MembershipChangeListener} triggers re-partitioning when nodes join or leave.</li>
 *   <li>A background worker simulates discovering and processing compensation records
 *       according to the current partition.</li>
 * </ol>
 *
 * <p>In a real system, the compensator would query a database for unprocessed records,
 * filter by partition, process those records, and record claims through the Raft
 * state machine for idempotency. This demo simulates the database with random
 * record generation and logs the work that would be done.</p>
 */
public class CompensatorAdapter extends BasicAdapter {

    private CompensatorStateMachine compensatorStateMachine;
    private final AtomicInteger myPosition = new AtomicInteger(-1);
    private final AtomicInteger totalCompensators = new AtomicInteger(0);
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final Random random = new Random();

    public static Builder builder(Peer me) {
        return new Builder(me);
    }

    public CompensatorAdapter(long timeoutMillis, Peer me, List<Peer> peers, Peer joinSeed) {
        super(timeoutMillis, me, peers, joinSeed);
    }

    public CompensatorAdapter(long timeoutMillis, Peer me, List<Peer> peers, Peer joinSeed, RuntimeConfiguration runtimeConfiguration) {
        super(timeoutMillis, me, peers, joinSeed, runtimeConfiguration);
    }

    public CompensatorAdapter(AdapterSpec spec) {
        super(spec);
    }

    @Override
    protected SnapshotStateMachine createSnapshotStateMachine() {
        compensatorStateMachine = new CompensatorStateMachine();
        return compensatorStateMachine;
    }

    @Override
    public void start() {
        super.start();

        stateMachine.addMembershipChangeListener(config -> {
            recomputePartition();
        });

        recomputePartition();

        startCompensatorWorker();
    }

    /**
     * Recomputes this node's position in the work partition from the current
     * cluster configuration. Called on startup and on every membership change.
     */
    private void recomputePartition() {
        RaftNode.TelemetrySnapshot snapshot = stateMachine.telemetrySnapshot();
        ClusterConfiguration config = snapshot.configuration();

        List<String> voters = new ArrayList<>(config.currentVotingMembers().stream()
                .map(Peer::getId)
                .sorted()
                .toList());

        int total = voters.size();
        int pos = voters.indexOf(me.getId());

        myPosition.set(pos);
        totalCompensators.set(total);

        if (pos < 0) {
            log.warn("Compensator {} is not a voting member (role: {}) — will not process work",
                    me.getId(), me.getRole());
        } else if (total <= 1) {
            log.info("Compensator {}: sole compensator (1 of 1) — processing all work",
                    me.getId());
        } else {
            log.info("Compensator {}: position {} of {} (partition: records where hash(id) % {} == {})",
                    me.getId(), pos, total, total, pos);
        }
    }

    private void startCompensatorWorker() {
        Thread worker = new Thread(() -> {
            while (running.get()) {
                try {
                    tick();
                    Thread.sleep(5_000L);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (RuntimeException e) {
                    log.warn("Compensator {} worker error", me.getId(), e);
                }
            }
        }, "compensator-worker-" + me.getId());
        worker.setDaemon(true);
        worker.start();
        log.info("Compensator {} started background worker", me.getId());
    }

    private void tick() {
        int pos = myPosition.get();
        int total = totalCompensators.get();

        if (pos < 0 || total <= 0) {
            return;
        }

        String recordId = "comp-" + me.getId() + "-" + System.currentTimeMillis() + "-" + random.nextInt(10_000);
        int bucket = Math.abs(recordId.hashCode()) % total;

        if (bucket != pos) {
            log.trace("Compensator {}: record {} belongs to bucket {} (mine is {}), skipping",
                    me.getId(), recordId, bucket, pos);
            return;
        }

        if (compensatorStateMachine.isClaimed(recordId)) {
            log.debug("Compensator {}: record {} already claimed, skipping", me.getId(), recordId);
            return;
        }

        byte[] claimCommand = CompensatorStateMachine.encodeClaim(me.getId(), recordId);

        if (stateMachine.isLeader()) {
            boolean accepted = stateMachine.submitCommand(claimCommand);
            if (accepted) {
                log.info("Compensator {} claimed compensation record {} (position {} of {})",
                        me.getId(), recordId, pos, total);
            } else {
                log.debug("Compensator {}: could not submit claim for {}", me.getId(), recordId);
            }
        } else {
            submitClaimToLeader(claimCommand, recordId, pos, total);
        }
    }

    private void submitClaimToLeader(byte[] claimCommand, String recordId, int pos, int total) {
        RaftNode.TelemetrySnapshot snapshot = stateMachine.telemetrySnapshot();
        String leaderId = snapshot.leaderId();
        if (leaderId == null || leaderId.isBlank()) {
            log.trace("Compensator {}: no known leader, deferring claim for {}", me.getId(), recordId);
            return;
        }

        Peer leaderPeer = snapshot.knownPeers().stream()
                .filter(p -> leaderId.equals(p.getId()))
                .findFirst()
                .orElse(null);

        if (leaderPeer == null) {
            log.trace("Compensator {}: leader {} not found in known peers, deferring claim for {}",
                    me.getId(), leaderId, recordId);
            return;
        }

        ClientCommandRequest request = new ClientCommandRequest(
                snapshot.term(), me.getId(), claimCommand,
                outboundRequestAuthScheme(), outboundRequestAuthToken()
        );

        stateMachine.getRaftClient().sendClientCommandRequest(leaderPeer, request)
                .thenAccept(response -> {
                    if (response != null && response.isSuccess()) {
                        log.info("Compensator {} claimed compensation record {} via leader {} (position {} of {})",
                                me.getId(), recordId, leaderId, pos, total);
                    } else {
                        log.debug("Compensator {}: leader {} rejected claim for {}",
                                me.getId(), leaderId, recordId);
                    }
                })
                .exceptionally(ex -> {
                    log.debug("Compensator {}: failed to send claim for {} to leader {}: {}",
                            me.getId(), recordId, leaderId, ex.getMessage());
                    return null;
                });
    }

    public int getMyPosition() {
        return myPosition.get();
    }

    public int getTotalCompensators() {
        return totalCompensators.get();
    }

    public CompensatorStateMachine getStateMachine() {
        return compensatorStateMachine;
    }

    public static final class Builder {
        private final AdapterSpec.Builder specBuilder;

        private Builder(Peer me) {
            this.specBuilder = AdapterSpec.forPeer(me);
        }

        public Builder withTimeoutMillis(long timeoutMillis) {
            specBuilder.withTimeoutMillis(timeoutMillis);
            return this;
        }

        public Builder withPeers(List<Peer> peers) {
            specBuilder.withPeers(peers);
            return this;
        }

        public Builder withJoinSeed(Peer joinSeed) {
            specBuilder.withJoinSeed(joinSeed);
            return this;
        }

        public Builder withRuntimeConfiguration(RuntimeConfiguration runtimeConfiguration) {
            specBuilder.withRuntimeConfiguration(runtimeConfiguration);
            return this;
        }

        public Builder withTransportFactory(RaftTransportFactory transportFactory) {
            specBuilder.withTransportFactory(transportFactory);
            return this;
        }

        public CompensatorAdapter build() {
            return new CompensatorAdapter(specBuilder.build());
        }
    }
}
