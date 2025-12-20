package org.gautelis.raft;

import org.gautelis.raft.model.LogEntry;

import java.util.ArrayList;
import java.util.List;

public final class InMemoryLogStore implements LogStore {

    private final List<LogEntry> log = new ArrayList<>();

    @Override
    public synchronized long lastIndex() {
        return log.size(); // 1-based externally: size==0 => lastIndex 0, size==N => lastIndex N
    }

    @Override
    public synchronized long lastTerm() {
        if (log.isEmpty()) return 0L;
        //return log.get(log.size() - 1).getTerm();
        return log.getLast().getTerm();
    }

    @Override
    public synchronized long termAt(long index) {
        if (index == 0) return 0L;
        int i = Math.toIntExact(index - 1); // 1-based -> 0-based
        if (i < 0 || i >= log.size()) {
            throw new IndexOutOfBoundsException("No entry at index " + index + " (size=" + log.size() + ")");
        }
        return log.get(i).getTerm();
    }

    @Override
    public synchronized void append(List<LogEntry> entries) {
        for (LogEntry entry : entries) {
            log.add(entry);
        }
    }
}
