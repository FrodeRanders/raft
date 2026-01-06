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

import java.util.List;

public interface LogStore {

    /**
     * Highest Raft log index present locally (1-based). 0 if empty.
     */
    long lastIndex();

    /**
     * Term of entry at lastIndex(); 0 if empty.
     */
    long lastTerm();

    /**
     * Term at a specific index; index==0 must return 0.
     */
    long termAt(long index);

    /**
     * Append entries to the log (entries must be COMMAND type).
     */
    void append(List<LogEntry> entries);
}
