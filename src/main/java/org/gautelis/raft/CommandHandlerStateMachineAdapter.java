package org.gautelis.raft;

final class CommandHandlerStateMachineAdapter implements SnapshotStateMachine {
    private final CommandHandler delegate;

    CommandHandlerStateMachineAdapter(CommandHandler delegate) {
        this.delegate = delegate;
    }

    @Override
    public void apply(long term, String command) {
        delegate.handle(term, command);
    }

    @Override
    public byte[] snapshot() {
        return new byte[0];
    }

    @Override
    public void restore(byte[] snapshotData) {
        // Compatibility adapter: legacy CommandHandler has no restore capability.
        // Snapshot bytes are intentionally ignored here.
    }
}
