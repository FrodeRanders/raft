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
package org.gautelis.raft.protocol;

import com.google.protobuf.InvalidProtocolBufferException;

import java.util.Optional;

public final class StateMachineQueryResult {
    public enum Type {
        GET
    }

    private final Type type;
    private final String key;
    private final boolean found;
    private final String value;

    private StateMachineQueryResult(Type type, String key, boolean found, String value) {
        this.type = type;
        this.key = key == null ? "" : key;
        this.found = found;
        this.value = value == null ? "" : value;
    }

    public static StateMachineQueryResult get(String key, boolean found, String value) {
        return new StateMachineQueryResult(Type.GET, key, found, value);
    }

    public Type getType() {
        return type;
    }

    public String getKey() {
        return key;
    }

    public boolean isFound() {
        return found;
    }

    public String getValue() {
        return value;
    }

    public byte[] encode() {
        var builder = org.gautelis.raft.proto.StateMachineQueryResult.newBuilder();
        if (type == Type.GET) {
            builder.setGet(org.gautelis.raft.proto.GetValueResult.newBuilder()
                    .setKey(key)
                    .setFound(found)
                    .setValue(value)
                    .build());
        }
        return builder.build().toByteArray();
    }

    public static Optional<StateMachineQueryResult> decode(byte[] payload) {
        try {
            var parsed = org.gautelis.raft.proto.StateMachineQueryResult.parseFrom(payload == null ? new byte[0] : payload);
            return switch (parsed.getResultCase()) {
                case GET -> Optional.of(get(parsed.getGet().getKey(), parsed.getGet().getFound(), parsed.getGet().getValue()));
                case RESULT_NOT_SET -> Optional.empty();
            };
        } catch (InvalidProtocolBufferException e) {
            return Optional.empty();
        }
    }
}
