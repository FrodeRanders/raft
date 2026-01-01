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
