package org.gautelis.raft;

import java.util.Optional;

public final class InMemoryPersistentStateStore implements PersistentStateStore {
    private long currentTerm;
    private String votedFor;

    @Override
    public synchronized long currentTerm() {
        return currentTerm;
    }

    @Override
    public synchronized void setCurrentTerm(long term) {
        this.currentTerm = term;
    }

    @Override
    public synchronized Optional<String> votedFor() {
        return Optional.ofNullable(votedFor);
    }

    @Override
    public synchronized void setVotedFor(String peerId) {
        this.votedFor = peerId;
    }
}
