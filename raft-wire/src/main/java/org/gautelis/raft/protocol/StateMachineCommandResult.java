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
 * Encodes and decodes typed command results returned after a write has committed and applied.
 */
public final class StateMachineCommandResult {
    public enum Type {
        CAS
    }

    private final Type type;
    private final String key;
    private final boolean matched;
    private final boolean expectedPresent;
    private final String expectedValue;
    private final String newValue;
    private final boolean currentPresent;
    private final String currentValue;

    private StateMachineCommandResult(Type type,
                                      String key,
                                      boolean matched,
                                      boolean expectedPresent,
                                      String expectedValue,
                                      String newValue,
                                      boolean currentPresent,
                                      String currentValue) {
        this.type = type;
        this.key = key == null ? "" : key;
        this.matched = matched;
        this.expectedPresent = expectedPresent;
        this.expectedValue = expectedValue == null ? "" : expectedValue;
        this.newValue = newValue == null ? "" : newValue;
        this.currentPresent = currentPresent;
        this.currentValue = currentValue == null ? "" : currentValue;
    }

    public static StateMachineCommandResult cas(String key,
                                                boolean expectedPresent,
                                                String expectedValue,
                                                String newValue,
                                                boolean matched,
                                                boolean currentPresent,
                                                String currentValue) {
        return new StateMachineCommandResult(Type.CAS, key, matched, expectedPresent, expectedValue, newValue, currentPresent, currentValue);
    }

    public Type getType() {
        return type;
    }

    public String getKey() {
        return key;
    }

    public boolean isMatched() {
        return matched;
    }

    public boolean isExpectedPresent() {
        return expectedPresent;
    }

    public String getExpectedValue() {
        return expectedValue;
    }

    public String getNewValue() {
        return newValue;
    }

    public boolean isCurrentPresent() {
        return currentPresent;
    }

    public String getCurrentValue() {
        return currentValue;
    }

    public byte[] encode() {
        var builder = org.gautelis.raft.proto.StateMachineCommandResult.newBuilder();
        switch (type) {
            case CAS -> builder.setCas(org.gautelis.raft.proto.CasCommandResult.newBuilder()
                    .setKey(key)
                    .setMatched(matched)
                    .setExpectedPresent(expectedPresent)
                    .setExpectedValue(expectedValue)
                    .setNewValue(newValue)
                    .setCurrentPresent(currentPresent)
                    .setCurrentValue(currentValue)
                    .build());
        }
        return builder.build().toByteArray();
    }

    public static Optional<StateMachineCommandResult> decode(byte[] payload) {
        try {
            var parsed = org.gautelis.raft.proto.StateMachineCommandResult.parseFrom(payload == null ? new byte[0] : payload);
            return switch (parsed.getResultCase()) {
                case CAS -> Optional.of(cas(
                        parsed.getCas().getKey(),
                        parsed.getCas().getExpectedPresent(),
                        parsed.getCas().getExpectedValue(),
                        parsed.getCas().getNewValue(),
                        parsed.getCas().getMatched(),
                        parsed.getCas().getCurrentPresent(),
                        parsed.getCas().getCurrentValue()
                ));
                case RESULT_NOT_SET -> Optional.empty();
            };
        } catch (InvalidProtocolBufferException e) {
            return Optional.empty();
        }
    }
}
