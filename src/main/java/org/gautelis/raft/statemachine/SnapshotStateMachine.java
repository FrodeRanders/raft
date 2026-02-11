package org.gautelis.raft.statemachine;

public interface SnapshotStateMachine {
    void apply(long term, String command);
    byte[] snapshot();
    void restore(byte[] snapshotData);
}
