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

import org.gautelis.raft.app.kv.KeyValueCliSupport;
import org.gautelis.raft.protocol.ClientCommandResponse;
import org.gautelis.raft.protocol.StateMachineCommandResult;
import org.gautelis.raft.protocol.ClientQueryResponse;
import org.gautelis.raft.protocol.StateMachineQueryResult;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

class KeyValueCliSupportTest {

    @Test
    void commandResponseJsonIncludesLeaderAndMessage() {
        ClientCommandResponse response = new ClientCommandResponse(
                5L,
                "A",
                true,
                "ACCEPTED",
                "Command committed and applied",
                "A",
                "127.0.0.1",
                10080
        );

        String json = KeyValueCliSupport.renderClientCommandResponseJson("put", response);

        assertTrue(json.contains("\"action\": \"put\""));
        assertTrue(json.contains("\"status\": \"ACCEPTED\""));
        assertTrue(json.contains("\"success\": true"));
        assertTrue(json.contains("\"leaderHost\": \"127.0.0.1\""));
        assertTrue(json.contains("\"leaderPort\": 10080"));
        assertTrue(json.contains("\"message\": \"Command committed and applied\""));
    }

    @Test
    void casCommandResponseJsonIncludesDecodedResult() {
        ClientCommandResponse response = new ClientCommandResponse(
                5L,
                "A",
                true,
                "ACCEPTED",
                "Command committed and applied",
                "A",
                "127.0.0.1",
                10080,
                StateMachineCommandResult.cas("x", true, "1", "2", true, true, "2").encode()
        );

        String json = KeyValueCliSupport.renderClientCommandResponseJson("cas", response);

        assertTrue(json.contains("\"action\": \"cas\""));
        assertTrue(json.contains("\"result\": {"));
        assertTrue(json.contains("\"type\": \"CAS\""));
        assertTrue(json.contains("\"key\": \"x\""));
        assertTrue(json.contains("\"matched\": true"));
        assertTrue(json.contains("\"expectedPresent\": true"));
        assertTrue(json.contains("\"expectedValue\": \"1\""));
        assertTrue(json.contains("\"newValue\": \"2\""));
        assertTrue(json.contains("\"currentPresent\": true"));
        assertTrue(json.contains("\"currentValue\": \"2\""));
    }

    @Test
    void casCommandResponseTextIncludesDecodedResult() {
        ClientCommandResponse response = new ClientCommandResponse(
                5L,
                "A",
                true,
                "ACCEPTED",
                "Command committed and applied",
                "A",
                "127.0.0.1",
                10080,
                StateMachineCommandResult.cas("x", false, "", "2", false, false, "").encode()
        );

        String rendered = KeyValueCliSupport.renderClientCommandResponse("cas", response);

        assertTrue(rendered.contains("result=CAS("));
        assertTrue(rendered.contains("key=x"));
        assertTrue(rendered.contains("matched=false"));
        assertTrue(rendered.contains("expected=<missing>"));
        assertTrue(rendered.contains("new=\"2\""));
        assertTrue(rendered.contains("current=<missing>"));
    }

    @Test
    void queryResponseJsonIncludesDecodedGetResult() {
        ClientQueryResponse response = new ClientQueryResponse(
                5L,
                "A",
                true,
                "OK",
                "Query completed",
                "A",
                "127.0.0.1",
                10080,
                StateMachineQueryResult.get("x", true, "42").encode()
        );

        String json = KeyValueCliSupport.renderClientQueryResponseJson(response);

        assertTrue(json.contains("\"status\": \"OK\""));
        assertTrue(json.contains("\"success\": true"));
        assertTrue(json.contains("\"peerId\": \"A\""));
        assertTrue(json.contains("\"result\": {"));
        assertTrue(json.contains("\"type\": \"GET\""));
        assertTrue(json.contains("\"key\": \"x\""));
        assertTrue(json.contains("\"found\": true"));
        assertTrue(json.contains("\"value\": \"42\""));
    }

    @Test
    void failedQueryResponseJsonUsesNullResult() {
        ClientQueryResponse response = new ClientQueryResponse(
                5L,
                "B",
                false,
                "REDIRECT",
                "Node is not leader; send query to current leader",
                "A",
                "127.0.0.1",
                10080,
                new byte[0]
        );

        String json = KeyValueCliSupport.renderClientQueryResponseJson(response);

        assertTrue(json.contains("\"status\": \"REDIRECT\""));
        assertTrue(json.contains("\"success\": false"));
        assertTrue(json.contains("\"result\": null"));
    }
}
