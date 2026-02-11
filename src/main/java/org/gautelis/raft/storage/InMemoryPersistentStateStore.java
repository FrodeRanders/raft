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
package org.gautelis.raft.storage;

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
