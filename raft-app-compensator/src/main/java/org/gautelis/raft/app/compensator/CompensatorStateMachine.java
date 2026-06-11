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

import org.gautelis.raft.statemachine.QueryableStateMachine;
import org.gautelis.raft.statemachine.ResultSnapshotStateMachine;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * State machine for the compensator work-partitioning demo.
 *
 * <p>The state tracks which compensation IDs have been claimed by each node.
 * This is replicated through Raft so all nodes agree on what has been done.
 * The actual database interaction is external to Raft and simulated here.</p>
 *
 * <p>The work partition itself (which node handles which IDs) is not stored
 * here; it is computed deterministically from the cluster membership:
 * {@code hash(recordId) % votingNodes.size() == myIndex}.</p>
 */
public class CompensatorStateMachine implements QueryableStateMachine, ResultSnapshotStateMachine {

    private final Map<String, String> lastClaimedByNode = new LinkedHashMap<>();
    private final Set<String> claimedIds = new LinkedHashSet<>();

    private static final byte TYPE_CLAIM = 1;
    private static final byte TYPE_QUERY_STATUS = 10;

    @Override
    public synchronized void apply(long term, byte[] command) {
        applyWithResult(term, command);
    }

    @Override
    public synchronized byte[] applyWithResult(long term, byte[] command) {
        if (command == null || command.length == 0) {
            return new byte[0];
        }
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(command))) {
            byte type = in.readByte();
            if (type == TYPE_CLAIM) {
                String nodeId = in.readUTF();
                String recordId = in.readUTF();
                claimedIds.add(recordId);
                lastClaimedByNode.put(nodeId, recordId);
                return encodeClaimResult(nodeId, recordId, true);
            }
        } catch (IOException e) {
            return new byte[0];
        }
        return new byte[0];
    }

    @Override
    public synchronized byte[] snapshot() {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream out = new DataOutputStream(baos)) {
            out.writeInt(lastClaimedByNode.size());
            for (Map.Entry<String, String> e : lastClaimedByNode.entrySet()) {
                out.writeUTF(e.getKey());
                out.writeUTF(e.getValue());
            }
            out.writeInt(claimedIds.size());
            for (String id : claimedIds) {
                out.writeUTF(id);
            }
            out.flush();
            return baos.toByteArray();
        } catch (IOException e) {
            throw new IllegalStateException("Failed creating state snapshot", e);
        }
    }

    @Override
    public synchronized void restore(byte[] snapshotData) {
        lastClaimedByNode.clear();
        claimedIds.clear();
        if (snapshotData == null || snapshotData.length == 0) {
            return;
        }
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(snapshotData))) {
            int nodeCount = in.readInt();
            for (int i = 0; i < nodeCount; i++) {
                lastClaimedByNode.put(in.readUTF(), in.readUTF());
            }
            int idCount = in.readInt();
            for (int i = 0; i < idCount; i++) {
                claimedIds.add(in.readUTF());
            }
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed restoring state snapshot", e);
        }
    }

    @Override
    public synchronized byte[] query(byte[] request) {
        if (request == null || request.length == 0) {
            return new byte[0];
        }
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(request))) {
            byte type = in.readByte();
            if (type == TYPE_QUERY_STATUS) {
                return encodeStatus();
            }
        } catch (IOException e) {
            return new byte[0];
        }
        return new byte[0];
    }

    public synchronized boolean isClaimed(String recordId) {
        return claimedIds.contains(recordId);
    }

    public synchronized int claimedCount() {
        return claimedIds.size();
    }

    public synchronized Map<String, String> getLastClaimedByNode() {
        return Collections.unmodifiableMap(new LinkedHashMap<>(lastClaimedByNode));
    }

    /**
     * Encodes a CLAIM command for the state machine.
     */
    public static byte[] encodeClaim(String nodeId, String recordId) {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream out = new DataOutputStream(baos)) {
            out.writeByte(TYPE_CLAIM);
            out.writeUTF(nodeId);
            out.writeUTF(recordId);
            out.flush();
            return baos.toByteArray();
        } catch (IOException e) {
            throw new IllegalStateException("Failed encoding claim command", e);
        }
    }

    /**
     * Encodes a STATUS query for the state machine.
     */
    public static byte[] encodeStatusQuery() {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream out = new DataOutputStream(baos)) {
            out.writeByte(TYPE_QUERY_STATUS);
            out.flush();
            return baos.toByteArray();
        } catch (IOException e) {
            throw new IllegalStateException("Failed encoding status query", e);
        }
    }

    private byte[] encodeClaimResult(String nodeId, String recordId, boolean success) {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream out = new DataOutputStream(baos)) {
            out.writeUTF(nodeId);
            out.writeUTF(recordId);
            out.writeBoolean(success);
            out.flush();
            return baos.toByteArray();
        } catch (IOException e) {
            return new byte[0];
        }
    }

    private byte[] encodeStatus() {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream out = new DataOutputStream(baos)) {
            out.writeInt(lastClaimedByNode.size());
            for (Map.Entry<String, String> e : lastClaimedByNode.entrySet()) {
                out.writeUTF(e.getKey());
                out.writeUTF(e.getValue());
            }
            out.writeInt(claimedIds.size());
            out.flush();
            return baos.toByteArray();
        } catch (IOException e) {
            return new byte[0];
        }
    }
}
