package org.gautelis.raft;

import org.gautelis.raft.model.LogEntry;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class InMemoryLogStoreTest {
    private static final Logger log = LoggerFactory.getLogger(InMemoryLogStoreTest.class);

    @Test
    void emptyLogDefaults() {
        log.info("*** Testcase *** Empty log defaults to zeros");

        InMemoryLogStore store = new InMemoryLogStore();
        assertEquals(0L, store.lastIndex());
        assertEquals(0L, store.lastTerm());
        assertEquals(0L, store.termAt(0));
    }

    @Test
    void appendUpdatesLastIndexAndTerm() {
        log.info("*** Testcase *** Append updates last index/term and termAt");

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
        log.info("*** Testcase *** termAt throws for out-of-bounds index");

        InMemoryLogStore store = new InMemoryLogStore();
        store.append(List.of(new LogEntry(1, "A")));

        assertThrows(IndexOutOfBoundsException.class, () -> store.termAt(2));
    }
}
