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

import org.gautelis.raft.protocol.StateMachineCommand;
import org.gautelis.raft.protocol.StateMachineCommandResult;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StateMachineCommandProtocolTest {

    @Test
    void casCommandRoundTripsThroughProtocolEncoding() {
        StateMachineCommand decoded = StateMachineCommand.decode(
                StateMachineCommand.cas("x", "1", "2").encode()
        ).orElseThrow();

        assertEquals(StateMachineCommand.Type.CAS, decoded.getType());
        assertEquals("x", decoded.getKey());
        assertTrue(decoded.isExpectedPresent());
        assertEquals("1", decoded.getExpectedValue());
        assertEquals("2", decoded.getValue());
    }

    @Test
    void casMissingCommandRoundTripsThroughProtocolEncoding() {
        StateMachineCommand decoded = StateMachineCommand.decode(
                StateMachineCommand.casMissing("x", "2").encode()
        ).orElseThrow();

        assertEquals(StateMachineCommand.Type.CAS, decoded.getType());
        assertEquals("x", decoded.getKey());
        assertFalse(decoded.isExpectedPresent());
        assertEquals("2", decoded.getValue());
    }

    @Test
    void casResultRoundTripsThroughProtocolEncoding() {
        StateMachineCommandResult decoded = StateMachineCommandResult.decode(
                StateMachineCommandResult.cas("x", true, "1", "2", false, true, "1").encode()
        ).orElseThrow();

        assertEquals(StateMachineCommandResult.Type.CAS, decoded.getType());
        assertEquals("x", decoded.getKey());
        assertFalse(decoded.isMatched());
        assertTrue(decoded.isExpectedPresent());
        assertEquals("1", decoded.getExpectedValue());
        assertEquals("2", decoded.getNewValue());
        assertTrue(decoded.isCurrentPresent());
        assertEquals("1", decoded.getCurrentValue());
    }
}
