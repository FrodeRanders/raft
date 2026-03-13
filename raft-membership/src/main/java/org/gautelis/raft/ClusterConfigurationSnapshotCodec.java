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

import org.gautelis.raft.protocol.Peer;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Wraps state-machine snapshots with Raft configuration metadata for persistence and install.
 */
final class ClusterConfigurationSnapshotCodec {
    private static final int VERSION = 1;

    record Decoded(ClusterConfiguration configuration, byte[] stateMachineSnapshot) {
    }

    private ClusterConfigurationSnapshotCodec() {
    }

    static byte[] encode(ClusterConfiguration configuration, byte[] stateMachineSnapshot) {
        // Persisted snapshot wrapper is JSON so config metadata remains explicit and debuggable
        // even though Raft RPCs themselves use protobuf on the wire.
        StringBuilder json = new StringBuilder();
        json.append('{');
        appendField(json, "version", Integer.toString(VERSION));
        json.append(',');
        appendField(json, "currentMembers", peersToJson(configuration.currentMembers()));
        json.append(',');
        appendField(json, "nextMembers", peersToJson(configuration.nextMembers()));
        json.append(',');
        String snapshot = Base64.getEncoder().encodeToString(stateMachineSnapshot == null ? new byte[0] : stateMachineSnapshot);
        appendField(json, "stateMachineSnapshot", quote(snapshot));
        json.append('}');
        return json.toString().getBytes(StandardCharsets.UTF_8);
    }

    static Optional<Decoded> decode(byte[] payload) {
        if (payload == null || payload.length == 0) {
            return Optional.empty();
        }

        String json = new String(payload, StandardCharsets.UTF_8).trim();
        if (!json.startsWith("{")) {
            return Optional.empty();
        }

        Object parsed = new JsonParser(json).parseValue();
        if (!(parsed instanceof Map<?, ?> root)) {
            throw new IllegalArgumentException("Configuration snapshot payload is not a JSON object");
        }

        int version = ((Number) required(root, "version")).intValue();
        if (version != VERSION) {
            throw new IllegalArgumentException("Unsupported configuration snapshot version " + version);
        }

        List<Peer> currentMembers = parsePeers(required(root, "currentMembers"));
        List<Peer> nextMembers = parsePeers(required(root, "nextMembers"));
        String snapshot = (String) required(root, "stateMachineSnapshot");

        // Stable snapshots have empty nextMembers; joint-consensus snapshots carry both sets.
        ClusterConfiguration configuration = ClusterConfiguration.stable(currentMembers);
        if (!nextMembers.isEmpty()) {
            configuration = configuration.transitionTo(nextMembers);
        }
        byte[] stateMachineSnapshot = Base64.getDecoder().decode(snapshot);
        return Optional.of(new Decoded(configuration, stateMachineSnapshot));
    }

    private static Object required(Map<?, ?> root, String key) {
        if (!root.containsKey(key)) {
            throw new IllegalArgumentException("Missing configuration snapshot field '" + key + "'");
        }
        return root.get(key);
    }

    private static String peersToJson(Iterable<Peer> peers) {
        StringBuilder json = new StringBuilder();
        json.append('[');
        boolean first = true;
        for (Peer peer : peers) {
            if (!first) {
                json.append(',');
            }
            first = false;
            json.append('{');
            appendField(json, "id", quote(peer.getId()));
            json.append(',');
            appendField(json, "role", quote(peer.getRole().name()));
            json.append(',');
            if (peer.getAddress() == null) {
                appendField(json, "address", "null");
            } else {
                appendField(json, "address", "{\"host\":" + quote(peer.getAddress().getHostString()) + ",\"port\":" + peer.getAddress().getPort() + '}');
            }
            json.append('}');
        }
        json.append(']');
        return json.toString();
    }

    private static List<Peer> parsePeers(Object value) {
        if (!(value instanceof List<?> list)) {
            throw new IllegalArgumentException("Peer list must be a JSON array");
        }
        List<Peer> peers = new ArrayList<>(list.size());
        for (Object item : list) {
            if (!(item instanceof Map<?, ?> peerObject)) {
                throw new IllegalArgumentException("Peer entry must be a JSON object");
            }
            String id = (String) required(peerObject, "id");
            String roleValue = peerObject.containsKey("role") ? (String) required(peerObject, "role") : Peer.Role.VOTER.name();
            Object addressValue = required(peerObject, "address");
            InetSocketAddress address = null;
            if (addressValue != null) {
                if (!(addressValue instanceof Map<?, ?> addressObject)) {
                    throw new IllegalArgumentException("Peer address must be a JSON object or null");
                }
                String host = (String) required(addressObject, "host");
                int port = ((Number) required(addressObject, "port")).intValue();
                address = new InetSocketAddress(host, port);
            }
            peers.add(new Peer(id, address, Peer.Role.valueOf(roleValue)));
        }
        return peers;
    }

    private static void appendField(StringBuilder json, String name, String rawValue) {
        json.append(quote(name)).append(':').append(rawValue);
    }

    private static String quote(String value) {
        StringBuilder escaped = new StringBuilder();
        escaped.append('"');
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
                case '"' -> escaped.append("\\\"");
                case '\\' -> escaped.append("\\\\");
                case '\b' -> escaped.append("\\b");
                case '\f' -> escaped.append("\\f");
                case '\n' -> escaped.append("\\n");
                case '\r' -> escaped.append("\\r");
                case '\t' -> escaped.append("\\t");
                default -> {
                    if (c < 0x20) {
                        escaped.append(String.format("\\u%04x", (int) c));
                    } else {
                        escaped.append(c);
                    }
                }
            }
        }
        escaped.append('"');
        return escaped.toString();
    }

    private static final class JsonParser {
        private final String json;
        private int index;

        JsonParser(String json) {
            this.json = json;
        }

        Object parseValue() {
            skipWhitespace();
            if (index >= json.length()) {
                throw new IllegalArgumentException("Unexpected end of JSON");
            }
            // Parser is intentionally tiny: it only needs the JSON subset emitted by encode().
            char c = json.charAt(index);
            return switch (c) {
                case '{' -> parseObject();
                case '[' -> parseArray();
                case '"' -> parseString();
                case 'n' -> parseNull();
                case 't', 'f' -> parseBoolean();
                default -> parseNumber();
            };
        }

        private Map<String, Object> parseObject() {
            expect('{');
            LinkedHashMap<String, Object> object = new LinkedHashMap<>();
            skipWhitespace();
            if (peek('}')) {
                expect('}');
                return object;
            }
            while (true) {
                String key = parseString();
                skipWhitespace();
                expect(':');
                object.put(key, parseValue());
                skipWhitespace();
                if (peek('}')) {
                    expect('}');
                    return object;
                }
                expect(',');
            }
        }

        private List<Object> parseArray() {
            expect('[');
            List<Object> array = new ArrayList<>();
            skipWhitespace();
            if (peek(']')) {
                expect(']');
                return array;
            }
            while (true) {
                array.add(parseValue());
                skipWhitespace();
                if (peek(']')) {
                    expect(']');
                    return array;
                }
                expect(',');
            }
        }

        private String parseString() {
            expect('"');
            StringBuilder value = new StringBuilder();
            while (index < json.length()) {
                char c = json.charAt(index++);
                if (c == '"') {
                    return value.toString();
                }
                if (c == '\\') {
                    char escaped = json.charAt(index++);
                    switch (escaped) {
                        case '"', '\\', '/' -> value.append(escaped);
                        case 'b' -> value.append('\b');
                        case 'f' -> value.append('\f');
                        case 'n' -> value.append('\n');
                        case 'r' -> value.append('\r');
                        case 't' -> value.append('\t');
                        case 'u' -> {
                            String hex = json.substring(index, index + 4);
                            value.append((char) Integer.parseInt(hex, 16));
                            index += 4;
                        }
                        default -> throw new IllegalArgumentException("Unsupported escape sequence \\" + escaped);
                    }
                } else {
                    value.append(c);
                }
            }
            throw new IllegalArgumentException("Unterminated JSON string");
        }

        private Object parseNull() {
            expectLiteral("null");
            return null;
        }

        private Boolean parseBoolean() {
            if (json.startsWith("true", index)) {
                index += 4;
                return Boolean.TRUE;
            }
            if (json.startsWith("false", index)) {
                index += 5;
                return Boolean.FALSE;
            }
            throw new IllegalArgumentException("Invalid JSON boolean");
        }

        private Number parseNumber() {
            int start = index;
            while (index < json.length()) {
                char c = json.charAt(index);
                if ((c >= '0' && c <= '9') || c == '-' || c == '+') {
                    index++;
                    continue;
                }
                break;
            }
            String token = json.substring(start, index);
            try {
                return Long.parseLong(token);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Invalid JSON number '" + token + "'", e);
            }
        }

        private void expectLiteral(String literal) {
            if (!json.startsWith(literal, index)) {
                throw new IllegalArgumentException("Expected '" + literal + "'");
            }
            index += literal.length();
        }

        private void expect(char expected) {
            skipWhitespace();
            if (index >= json.length() || json.charAt(index) != expected) {
                throw new IllegalArgumentException("Expected '" + expected + "'");
            }
            index++;
        }

        private boolean peek(char expected) {
            skipWhitespace();
            return index < json.length() && json.charAt(index) == expected;
        }

        private void skipWhitespace() {
            while (index < json.length() && Character.isWhitespace(json.charAt(index))) {
                index++;
            }
        }
    }
}
