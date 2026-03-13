/*
 * Copyright (C) 2025-2026 Frode Randers
 * All rights reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gautelis.raft.protocol;

import com.google.protobuf.InvalidProtocolBufferException;

import java.util.Optional;

/**
 * Encodes and decodes the typed read queries understood by the demo state machine.
 */
public final class StateMachineQuery {
    public enum Type {
        GET
    }

    private final Type type;
    private final String key;

    private StateMachineQuery(Type type, String key) {
        this.type = type;
        this.key = key == null ? "" : key;
    }

    public static StateMachineQuery get(String key) {
        return new StateMachineQuery(Type.GET, key);
    }

    public Type getType() {
        return type;
    }

    public String getKey() {
        return key;
    }

    public byte[] encode() {
        var builder = org.gautelis.raft.proto.StateMachineQuery.newBuilder();
        if (type == Type.GET) {
            builder.setGet(org.gautelis.raft.proto.GetValueQuery.newBuilder().setKey(key).build());
        }
        return builder.build().toByteArray();
    }

    public static Optional<StateMachineQuery> decode(byte[] payload) {
        try {
            var parsed = org.gautelis.raft.proto.StateMachineQuery.parseFrom(payload == null ? new byte[0] : payload);
            return switch (parsed.getQueryCase()) {
                case GET -> Optional.of(get(parsed.getGet().getKey()));
                case QUERY_NOT_SET -> Optional.empty();
            };
        } catch (InvalidProtocolBufferException e) {
            return Optional.empty();
        }
    }
}
