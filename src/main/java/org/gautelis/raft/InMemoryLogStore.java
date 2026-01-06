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
