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

import org.gautelis.raft.app.kv.KeyValueStateMachine;
import org.gautelis.raft.protocol.StateMachineCommand;
import org.gautelis.raft.protocol.StateMachineQuery;
import org.gautelis.raft.protocol.StateMachineQueryResult;
import org.gautelis.raft.storage.*;
import org.gautelis.raft.statemachine.*;
import org.gautelis.raft.transport.netty.*;
import org.gautelis.raft.serialization.ProtoMapper;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KeyValueStateMachineTest {
    private static final Logger log = LoggerFactory.getLogger(KeyValueStateMachineTest.class);

    @Test
    void applyAndSnapshotRestoreRoundtrip() {
        log.info("*** Testcase *** KeyValue state machine roundtrip: verifies apply mutations survive snapshot/restore with deterministic state");
        KeyValueStateMachine sm = new KeyValueStateMachine();
        sm.apply(1, StateMachineCommand.put("a", "1").encode());
        sm.apply(1, StateMachineCommand.put("b", "hello").encode());
        sm.apply(1, StateMachineCommand.delete("a").encode());

        assertNull(sm.get("a"));
        assertEquals("hello", sm.get("b"));

        byte[] snapshot = sm.snapshot();
        KeyValueStateMachine restored = new KeyValueStateMachine();
        restored.restore(snapshot);

        assertNull(restored.get("a"));
        assertEquals("hello", restored.get("b"));
    }

    @Test
    void queryReturnsTypedGetResult() {
        log.info("*** Testcase *** KeyValue query result: verifies structured get query returns found/value state");
        KeyValueStateMachine sm = new KeyValueStateMachine();
        sm.apply(1, StateMachineCommand.put("a", "1").encode());

        StateMachineQueryResult result = StateMachineQueryResult.decode(sm.query(StateMachineQuery.get("a").encode())).orElseThrow();
        StateMachineQueryResult missing = StateMachineQueryResult.decode(sm.query(StateMachineQuery.get("missing").encode())).orElseThrow();

        assertEquals("a", result.getKey());
        assertEquals("1", result.getValue());
        assertTrue(result.isFound());
        assertFalse(missing.isFound());
    }
}
