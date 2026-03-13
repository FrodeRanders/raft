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
package org.gautelis.raft.bootstrap;

import org.gautelis.raft.protocol.ClientCommandResponse;
import org.gautelis.raft.protocol.ClientQueryResponse;
import org.gautelis.raft.protocol.ClusterSummaryResponse;
import org.gautelis.raft.protocol.JoinClusterResponse;
import org.gautelis.raft.protocol.JoinClusterStatusResponse;
import org.gautelis.raft.protocol.ReconfigurationStatusResponse;
import org.gautelis.raft.protocol.ReconfigureClusterResponse;
import org.gautelis.raft.protocol.TelemetryResponse;
import org.gautelis.raft.serialization.ProtoMapper;
import org.gautelis.raft.transport.MessageResponder;

final class AdapterResponseWriters {
    private AdapterResponseWriters() {
    }

    static void writeJoinResponse(MessageResponder responder, String correlationId, JoinClusterResponse response) {
        write(responder, correlationId, response, "JoinClusterResponse", ProtoMapper.toProto(response).toByteString().toByteArray());
    }

    static void writeClientCommandResponse(MessageResponder responder, String correlationId, ClientCommandResponse response) {
        write(responder, correlationId, response, "ClientCommandResponse", ProtoMapper.toProto(response).toByteString().toByteArray());
    }

    static void writeClientQueryResponse(MessageResponder responder, String correlationId, ClientQueryResponse response) {
        write(responder, correlationId, response, "ClientQueryResponse", ProtoMapper.toProto(response).toByteString().toByteArray());
    }

    static void writeJoinStatusResponse(MessageResponder responder, String correlationId, JoinClusterStatusResponse response) {
        write(responder, correlationId, response, "JoinClusterStatusResponse", ProtoMapper.toProto(response).toByteString().toByteArray());
    }

    static void writeReconfigureResponse(MessageResponder responder, String correlationId, ReconfigureClusterResponse response) {
        write(responder, correlationId, response, "ReconfigureClusterResponse", ProtoMapper.toProto(response).toByteString().toByteArray());
    }

    static void writeTelemetryResponse(MessageResponder responder, String correlationId, TelemetryResponse response) {
        write(responder, correlationId, response, "TelemetryResponse", ProtoMapper.toProto(response).toByteString().toByteArray());
    }

    static void writeClusterSummaryResponse(MessageResponder responder, String correlationId, ClusterSummaryResponse response) {
        write(responder, correlationId, response, "ClusterSummaryResponse", ProtoMapper.toProto(response).toByteString().toByteArray());
    }

    static void writeReconfigurationStatusResponse(MessageResponder responder, String correlationId, ReconfigurationStatusResponse response) {
        write(responder, correlationId, response, "ReconfigurationStatusResponse", ProtoMapper.toProto(response).toByteString().toByteArray());
    }

    private static void write(MessageResponder responder, String correlationId, Object response, String type, byte[] payload) {
        if (responder == null || correlationId == null || correlationId.isBlank() || response == null) {
            return;
        }
        responder.write(correlationId, type, payload);
    }
}
