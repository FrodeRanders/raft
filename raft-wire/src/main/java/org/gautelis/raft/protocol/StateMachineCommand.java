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

/**
 * Encodes and decodes the typed write commands understood by the demo state machine.
 */
public final class StateMachineCommand {
    public enum Type {
        PUT,
        DELETE,
        CLEAR
    }

    private final Type type;
    private final String key;
    private final String value;

    private StateMachineCommand(Type type, String key, String value) {
        this.type = type;
        this.key = key == null ? "" : key;
        this.value = value == null ? "" : value;
    }

    public static StateMachineCommand put(String key, String value) {
        return new StateMachineCommand(Type.PUT, key, value);
    }

    public static StateMachineCommand delete(String key) {
        return new StateMachineCommand(Type.DELETE, key, "");
    }

    public static StateMachineCommand clear() {
        return new StateMachineCommand(Type.CLEAR, "", "");
    }

    public Type getType() {
        return type;
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    public byte[] encode() {
        var builder = org.gautelis.raft.proto.StateMachineCommand.newBuilder();
        switch (type) {
            case PUT -> builder.setPut(org.gautelis.raft.proto.PutCommand.newBuilder()
                    .setKey(key)
                    .setValue(value)
                    .build());
            case DELETE -> builder.setDelete(org.gautelis.raft.proto.DeleteCommand.newBuilder()
                    .setKey(key)
                    .build());
            case CLEAR -> builder.setClear(org.gautelis.raft.proto.ClearCommand.newBuilder().build());
        }
        return builder.build().toByteArray();
    }

    @Override
    public String toString() {
        return switch (type) {
            case PUT -> "PUT(" + key + "=" + value + ")";
            case DELETE -> "DELETE(" + key + ")";
            case CLEAR -> "CLEAR";
        };
    }

    public static Optional<StateMachineCommand> decode(byte[] payload) {
        try {
            var parsed = org.gautelis.raft.proto.StateMachineCommand.parseFrom(payload == null ? new byte[0] : payload);
            return switch (parsed.getCommandCase()) {
                case PUT -> Optional.of(put(parsed.getPut().getKey(), parsed.getPut().getValue()));
                case DELETE -> Optional.of(delete(parsed.getDelete().getKey()));
                case CLEAR -> Optional.of(clear());
                case COMMAND_NOT_SET -> Optional.empty();
            };
        } catch (InvalidProtocolBufferException e) {
            return Optional.empty();
        }
    }
}
