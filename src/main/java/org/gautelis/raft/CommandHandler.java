package org.gautelis.raft;

public interface CommandHandler {
    void handle(long myTerm, String command);
}
