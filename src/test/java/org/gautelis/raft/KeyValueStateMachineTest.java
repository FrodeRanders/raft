package org.gautelis.raft;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class KeyValueStateMachineTest {
    private static final Logger log = LoggerFactory.getLogger(KeyValueStateMachineTest.class);

    @Test
    void applyAndSnapshotRestoreRoundtrip() {
        log.info("*** Testcase *** KeyValue state machine roundtrip: verifies apply mutations survive snapshot/restore with deterministic state");
        KeyValueStateMachine sm = new KeyValueStateMachine();
        sm.apply(1, "set a 1");
        sm.apply(1, "put b hello");
        sm.apply(1, "del a");

        assertNull(sm.get("a"));
        assertEquals("hello", sm.get("b"));

        byte[] snapshot = sm.snapshot();
        KeyValueStateMachine restored = new KeyValueStateMachine();
        restored.restore(snapshot);

        assertNull(restored.get("a"));
        assertEquals("hello", restored.get("b"));
    }
}
