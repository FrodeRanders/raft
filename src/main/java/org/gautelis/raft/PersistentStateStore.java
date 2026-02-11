package org.gautelis.raft;

import java.util.Optional;

public interface PersistentStateStore {
    long currentTerm();
    void setCurrentTerm(long term);
    Optional<String> votedFor();
    void setVotedFor(String peerId);
}
