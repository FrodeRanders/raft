/*
 * Copyright (C) 2025-2026 Frode Randers
 * All rights reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gautelis.raft.statemachine;

public final class CommandHandlerStateMachineAdapter implements SnapshotStateMachine {
    private final CommandHandler delegate;

    public CommandHandlerStateMachineAdapter(CommandHandler delegate) {
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
