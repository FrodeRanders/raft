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
package org.gautelis.raft.storage;

import org.gautelis.raft.protocol.LogEntry;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public final class InMemoryLogStore implements LogStore {
    // Simple, test-friendly storage backend.
    // Global indices remain stable across compaction through snapshotIndex offset.
    // Figure 2 mapping: provides prevLog term/index checks and conflict truncation support.
    // Figure 3 mapping: stable global indices preserve log-matching reasoning after compaction.

    private final List<LogEntry> log = new ArrayList<>();
    private long snapshotIndex = 0L;
    private long snapshotTerm = 0L;
    private byte[] snapshotData = new byte[0];

    @Override
    public synchronized long snapshotIndex() {
        return snapshotIndex;
    }

    @Override
    public synchronized long snapshotTerm() {
        return snapshotTerm;
    }

    @Override
    public synchronized long lastIndex() {
        return snapshotIndex + log.size();
    }

    @Override
    public synchronized long lastTerm() {
        if (log.isEmpty()) return snapshotTerm;
        return log.getLast().getTerm();
    }

    @Override
    public synchronized long termAt(long index) {
        // Index 0 is the Raft sentinel used by empty-prefix AppendEntries checks.
        if (index == 0) return 0L;
        if (index == snapshotIndex) {
            return snapshotTerm;
        }
        if (index < snapshotIndex) {
            throw new IndexOutOfBoundsException("No entry at compacted index " + index + " (snapshotIndex=" + snapshotIndex + ")");
        }
        int i = Math.toIntExact(index - snapshotIndex - 1); // global -> local 0-based
        if (i < 0 || i >= log.size()) {
            throw new IndexOutOfBoundsException("No entry at index " + index + " (snapshotIndex=" + snapshotIndex + ", size=" + log.size() + ")");
        }
        return log.get(i).getTerm();
    }

    @Override
    public synchronized LogEntry entryAt(long index) {
        if (index == 0 || index <= snapshotIndex) {
            throw new IndexOutOfBoundsException("No entry at index 0");
        }
        int i = Math.toIntExact(index - snapshotIndex - 1);
        if (i < 0 || i >= log.size()) {
            throw new IndexOutOfBoundsException("No entry at index " + index + " (snapshotIndex=" + snapshotIndex + ", size=" + log.size() + ")");
        }
        return log.get(i);
    }

    @Override
    public synchronized void append(List<LogEntry> entries) {
        for (LogEntry entry : entries) {
            log.add(entry);
        }
    }

    @Override
    public synchronized void truncateFrom(long index) {
        // Figure 2 AppendEntries receiver step 3:
        // delete conflicting entry and all that follow it.
        if (index <= 0) {
            log.clear();
            return;
        }
        if (index <= snapshotIndex + 1) {
            log.clear();
            return;
        }
        int from = Math.toIntExact(index - snapshotIndex - 1);
        if (from >= log.size()) {
            return;
        }
        log.subList(from, log.size()).clear();
    }

    @Override
    public synchronized List<LogEntry> entriesFrom(long index) {
        if (index <= snapshotIndex) {
            index = snapshotIndex + 1;
        }
        int from = Math.toIntExact(index - snapshotIndex - 1);
        if (from >= log.size()) {
            return List.of();
        }
        return new ArrayList<>(log.subList(from, log.size()));
    }

    @Override
    public synchronized void compactUpTo(long index) {
        // Prefix compaction keeps a snapshot boundary (snapshotIndex/snapshotTerm)
        // so termAt(snapshotIndex) still works for future consistency checks.
        if (index <= snapshotIndex) {
            return;
        }
        long target = Math.min(index, lastIndex());
        long targetTerm = termAt(target);

        if (target >= lastIndex()) {
            log.clear();
            snapshotIndex = target;
            snapshotTerm = targetTerm;
            return;
        }

        int removeCount = Math.toIntExact(target - snapshotIndex);
        log.subList(0, removeCount).clear();
        snapshotIndex = target;
        snapshotTerm = targetTerm;
        snapshotData = new byte[0];
    }

    @Override
    public synchronized byte[] snapshotData() {
        return Arrays.copyOf(snapshotData, snapshotData.length);
    }

    @Override
    public synchronized void installSnapshot(long lastIncludedIndex, long lastIncludedTerm, byte[] snapshotData) {
        // Keep suffix only when snapshot point matches local term/index;
        // otherwise drop suffix as conflicting history.
        if (lastIncludedIndex < snapshotIndex) {
            return;
        }

        long previousSnapshotIndex = snapshotIndex;
        List<LogEntry> suffix = List.of();
        if (lastIncludedIndex < lastIndex()) {
            if (lastIncludedIndex >= previousSnapshotIndex && termAt(lastIncludedIndex) == lastIncludedTerm) {
                suffix = entriesFrom(lastIncludedIndex + 1);
            }
        }

        log.clear();
        log.addAll(suffix);
        snapshotIndex = lastIncludedIndex;
        snapshotTerm = lastIncludedTerm;
        this.snapshotData = snapshotData == null ? new byte[0] : Arrays.copyOf(snapshotData, snapshotData.length);
    }
}
