package org.gautelis.raft.storage;

import java.util.Optional;

public interface PersistentStateStore {
    long currentTerm();
    void setCurrentTerm(long term);
    Optional<String> votedFor();
    void setVotedFor(String peerId);
}
