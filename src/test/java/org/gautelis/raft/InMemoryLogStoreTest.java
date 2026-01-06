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
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class InMemoryLogStoreTest {
    @Test
    void emptyLogDefaults() {
        InMemoryLogStore store = new InMemoryLogStore();
        assertEquals(0L, store.lastIndex());
        assertEquals(0L, store.lastTerm());
        assertEquals(0L, store.termAt(0));
    }

    @Test
    void appendUpdatesLastIndexAndTerm() {
        InMemoryLogStore store = new InMemoryLogStore();
        store.append(List.of(
                new LogEntry(1, "A"),
                new LogEntry(2, "A")
        ));

        assertEquals(2L, store.lastIndex());
        assertEquals(2L, store.lastTerm());
        assertEquals(1L, store.termAt(1));
        assertEquals(2L, store.termAt(2));
    }

    @Test
    void termAtOutOfBoundsThrows() {
        InMemoryLogStore store = new InMemoryLogStore();
        store.append(List.of(new LogEntry(1, "A")));

        assertThrows(IndexOutOfBoundsException.class, () -> store.termAt(2));
    }
}
