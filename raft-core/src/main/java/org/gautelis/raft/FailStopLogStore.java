package org.gautelis.raft;

import java.util.List;
import java.util.function.Supplier;
import org.gautelis.raft.protocol.LogEntry;
import org.gautelis.raft.storage.LogStore;

/** A failed durable operation permanently fences its containing node. */
final class FailStopLogStore implements LogStore {
    private final LogStore delegate;
    private final Runnable fail;
    private boolean failed;

    FailStopLogStore(LogStore delegate, Runnable fail) { this.delegate = delegate; this.fail = fail; }

    private synchronized <T> T access(Supplier<T> operation) {
        if (failed) throw new IllegalStateException("Raft storage is fenced after an earlier failure");
        try { return operation.get(); }
        catch (RuntimeException error) { failed = true; fail.run(); throw error; }
    }
    public long snapshotIndex() { return access(delegate::snapshotIndex); }
    public long snapshotTerm() { return access(delegate::snapshotTerm); }
    public long lastIndex() { return access(delegate::lastIndex); }
    public long lastTerm() { return access(delegate::lastTerm); }
    public long termAt(long index) { return access(() -> delegate.termAt(index)); }
    public LogEntry entryAt(long index) { return access(() -> delegate.entryAt(index)); }
    public List<LogEntry> entriesFrom(long index) { return access(() -> delegate.entriesFrom(index)); }
    public byte[] snapshotData() { return access(delegate::snapshotData); }
    public void append(List<LogEntry> entries) { access(() -> { delegate.append(entries); return null; }); }
    public void truncateFrom(long index) { access(() -> { delegate.truncateFrom(index); return null; }); }
    public void compactUpTo(long index) { access(() -> { delegate.compactUpTo(index); return null; }); }
    public void installSnapshot(long index, long term, byte[] bytes) {
        access(() -> { delegate.installSnapshot(index, term, bytes); return null; });
    }
}
