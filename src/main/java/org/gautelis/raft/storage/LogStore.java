package org.gautelis.raft.storage;

import org.gautelis.raft.protocol.LogEntry;

import java.util.List;

public interface LogStore {
    // Storage contract used by Raft paper Figure 2 log replication rules:
    // - term/index queries for prevLog checks
    // - append/truncate for conflict resolution
    // - snapshot metadata + install for compaction/catch-up
    // Figure 3 linkage:
    // - preserving exact term/index history underpins Log Matching and State Machine Safety.

    /**
     * Highest compacted index included in local snapshot metadata. 0 if none.
     */
    long snapshotIndex();

    /**
     * Term of snapshotIndex(); 0 if none.
     */
    long snapshotTerm();

    /**
     * Highest Raft log index present locally (1-based). 0 if empty.
     */
    long lastIndex();

    /**
     * Term of entry at lastIndex(); 0 if empty.
     */
    long lastTerm();

    /**
     * Term at a specific index; index==0 must return 0.
     */
    long termAt(long index);

    /**
     * Entry at a specific index (1-based).
     */
    LogEntry entryAt(long index);

    /**
     * Append entries to the log (entries must be COMMAND type).
     */
    void append(List<LogEntry> entries);

    /**
     * Remove entries from index (1-based) to end.
     */
    void truncateFrom(long index);

    /**
     * Return a copy of entries starting from index (1-based). Empty if index > lastIndex.
     */
    List<LogEntry> entriesFrom(long index);

    /**
     * Compact prefix up to and including index. Must preserve snapshot metadata so termAt(snapshotIndex()) works.
     */
    void compactUpTo(long index);

    /**
     * Snapshot payload bytes for transfer. Empty if no payload.
     */
    byte[] snapshotData();

    /**
     * Install snapshot metadata/payload and drop conflicting compacted log prefix.
     */
    void installSnapshot(long lastIncludedIndex, long lastIncludedTerm, byte[] snapshotData);
}
