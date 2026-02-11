package org.gautelis.raft.statemachine;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

public final class KeyValueStateMachine implements SnapshotStateMachine {
    // Reference implementation of a deterministic state machine used by the Raft log:
    // - apply() mutates authoritative state from committed commands
    // - snapshot()/restore() provides compaction and InstallSnapshot support
    // Figure 3 linkage: deterministic apply order is required for State Machine Safety.
    private final Map<String, String> values = new HashMap<>();

    @Override
    public synchronized void apply(long term, String command) {
        // Only committed commands should arrive here from RaftNode.applyCommittedEntries().
        if (command == null) {
            return;
        }

        String trimmed = command.trim();
        if (trimmed.isEmpty()) {
            return;
        }

        String[] parts = trimmed.split("\\s+", 3);
        String op = parts[0].toLowerCase(java.util.Locale.ROOT);
        switch (op) {
            case "set", "put" -> {
                if (parts.length >= 3) {
                    values.put(parts[1], parts[2]);
                }
            }
            case "delete", "del" -> {
                if (parts.length >= 2) {
                    values.remove(parts[1]);
                }
            }
            case "clear" -> values.clear();
            default -> {
                // Unknown command is ignored.
            }
        }
    }

    @Override
    public synchronized byte[] snapshot() {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream out = new DataOutputStream(baos)) {
            // Deterministic key ordering ensures byte-stable snapshots for equal state.
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
        // Restore replaces entire local state with snapshot image from leader/local compaction.
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

    public synchronized Map<String, String> asMap() {
        return Collections.unmodifiableMap(new HashMap<>(values));
    }
}
