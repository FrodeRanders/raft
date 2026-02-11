package org.gautelis.raft.statemachine;

public interface CommandHandler {
    void handle(long myTerm, String command);
}
