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
package org.gautelis.raft.app.kv;

import org.gautelis.raft.protocol.StateMachineCommand;
import org.gautelis.raft.protocol.StateMachineCommandResult;
import org.gautelis.raft.protocol.StateMachineQuery;
import org.gautelis.raft.protocol.StateMachineQueryResult;
import org.gautelis.raft.statemachine.QueryableStateMachine;
import org.gautelis.raft.statemachine.ResultSnapshotStateMachine;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * Demo key-value application state machine kept outside the generic Raft runtime layer.
 */
public class KeyValueStateMachine implements QueryableStateMachine, ResultSnapshotStateMachine {
    private final Map<String, String> values = new HashMap<>();

    @Override
    public synchronized void apply(long term, byte[] command) {
        applyWithResult(term, command);
    }

    @Override
    public synchronized byte[] applyWithResult(long term, byte[] command) {
        if (command == null || command.length == 0) {
            return new byte[0];
        }
        var parsed = StateMachineCommand.decode(command);
        if (parsed.isEmpty()) {
            return new byte[0];
        }
        StateMachineCommand decoded = parsed.get();
        return switch (decoded.getType()) {
            case PUT -> {
                values.put(decoded.getKey(), decoded.getValue());
                yield new byte[0];
            }
            case CAS -> {
                boolean currentPresent = values.containsKey(decoded.getKey());
                String currentValue = values.get(decoded.getKey());
                boolean matched = decoded.isExpectedPresent()
                        ? currentPresent && decoded.getExpectedValue().equals(currentValue)
                        : !currentPresent;
                if (matched) {
                    values.put(decoded.getKey(), decoded.getValue());
                    currentPresent = true;
                    currentValue = decoded.getValue();
                }
                yield StateMachineCommandResult.cas(
                        decoded.getKey(),
                        decoded.isExpectedPresent(),
                        decoded.getExpectedValue(),
                        decoded.getValue(),
                        matched,
                        currentPresent,
                        currentValue
                ).encode();
            }
            case DELETE -> {
                values.remove(decoded.getKey());
                yield new byte[0];
            }
            case CLEAR -> {
                values.clear();
                yield new byte[0];
            }
        };
    }

    @Override
    public synchronized byte[] snapshot() {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream out = new DataOutputStream(baos)) {
            Map<String, String> ordered = new TreeMap<>(values);
            out.writeInt(ordered.size());
            for (Map.Entry<String, String> e : ordered.entrySet()) {
                out.writeUTF(e.getKey());
                out.writeUTF(e.getValue());
            }
            out.flush();
            return baos.toByteArray();
        } catch (IOException e) {
            throw new IllegalStateException("Failed creating state snapshot", e);
        }
    }

    @Override
    public synchronized void restore(byte[] snapshotData) {
        values.clear();
        if (snapshotData == null || snapshotData.length == 0) {
            return;
        }

        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(snapshotData))) {
            int size = in.readInt();
            for (int i = 0; i < size; i++) {
                String key = in.readUTF();
                String value = in.readUTF();
                values.put(key, value);
            }
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed restoring state snapshot", e);
        }
    }

    public synchronized String get(String key) {
        return values.get(key);
    }

    @Override
    public synchronized byte[] query(byte[] request) {
        var parsed = StateMachineQuery.decode(request);
        if (parsed.isEmpty()) {
            return new byte[0];
        }
        StateMachineQuery query = parsed.get();
        return switch (query.getType()) {
            case GET -> StateMachineQueryResult.get(query.getKey(), values.containsKey(query.getKey()), values.get(query.getKey())).encode();
        };
    }

    public synchronized Map<String, String> asMap() {
        return Collections.unmodifiableMap(new HashMap<>(values));
    }
}
