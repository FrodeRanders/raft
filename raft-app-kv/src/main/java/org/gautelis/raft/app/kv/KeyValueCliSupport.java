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
package org.gautelis.raft.app.kv;

import org.gautelis.raft.protocol.ClientCommandRequest;
import org.gautelis.raft.protocol.ClientCommandResponse;
import org.gautelis.raft.protocol.ClientQueryRequest;
import org.gautelis.raft.protocol.ClientQueryResponse;
import org.gautelis.raft.protocol.Peer;
import org.gautelis.raft.protocol.StateMachineCommand;
import org.gautelis.raft.protocol.StateMachineQuery;
import org.gautelis.raft.protocol.StateMachineQueryResult;
import org.gautelis.raft.transport.RaftTransportClient;

import java.net.InetSocketAddress;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import org.gautelis.raft.bootstrap.CliRuntimeContext;

/**
 * CLI helpers for the key-value demo application.
 */
public final class KeyValueCliSupport {
    private KeyValueCliSupport() {
    }

    public static void runCommand(String[] args,
                                  CliRuntimeContext context) {
        boolean json = false;
        int actionIndex = 1;
        if (args.length > 1 && "--json".equalsIgnoreCase(args[1])) {
            json = true;
            actionIndex = 2;
        }
        if (args.length < actionIndex + 2) {
            System.err.println("Usage: java -jar raft-dist/target/raft-1.0-SNAPSHOT.jar command [--json] <put|delete|clear> <target-port|target-host:port|id@target-host:port> [key] [value]");
            System.exit(1);
        }

        String action = args[actionIndex].trim().toLowerCase(Locale.ROOT);
        Peer target = context.parsePeer().apply(args[actionIndex + 1]);
        byte[] command;
        switch (action) {
            case "put" -> {
                if (args.length != actionIndex + 4) {
                    System.err.println("Usage: java -jar raft-dist/target/raft-1.0-SNAPSHOT.jar command [--json] put <target> <key> <value>");
                    System.exit(1);
                    return;
                }
                command = StateMachineCommand.put(args[actionIndex + 2], args[actionIndex + 3]).encode();
            }
            case "delete" -> {
                if (args.length != actionIndex + 3) {
                    System.err.println("Usage: java -jar raft-dist/target/raft-1.0-SNAPSHOT.jar command [--json] delete <target> <key>");
                    System.exit(1);
                    return;
                }
                command = StateMachineCommand.delete(args[actionIndex + 2]).encode();
            }
            case "clear" -> {
                if (args.length != actionIndex + 2) {
                    System.err.println("Usage: java -jar raft-dist/target/raft-1.0-SNAPSHOT.jar command [--json] clear <target>");
                    System.exit(1);
                    return;
                }
                command = StateMachineCommand.clear().encode();
            }
            default -> {
                System.err.println("Unknown command action: " + args[1]);
                System.exit(1);
                return;
            }
        }

        RaftTransportClient client = context.newClient("command-cli");
        try {
            ClientCommandRequest request = new ClientCommandRequest(
                    0L,
                    "command-cli",
                    command,
                    context.authScheme().get(),
                    context.authToken().get()
            );
            ClientCommandResponse response = client.sendClientCommandRequest(target, request).get(5, TimeUnit.SECONDS);
            if ("REDIRECT".equals(response.getStatus()) && !response.getLeaderHost().isBlank() && response.getLeaderPort() > 0) {
                Peer leader = new Peer(response.getLeaderId(), new InetSocketAddress(response.getLeaderHost(), response.getLeaderPort()));
                response = client.sendClientCommandRequest(leader, request).get(5, TimeUnit.SECONDS);
            }
            System.out.println(json ? renderClientCommandResponseJson(action, response) : renderClientCommandResponse(action, response));
            if (!response.isSuccess()) {
                System.exit(2);
            }
        } catch (Exception e) {
            System.err.println("Command request failed: " + e.getMessage());
            System.exit(2);
        } finally {
            client.shutdown();
        }
    }

    public static void runQuery(String[] args,
                                CliRuntimeContext context) {
        boolean json = false;
        int actionIndex = 1;
        if (args.length > 1 && "--json".equalsIgnoreCase(args[1])) {
            json = true;
            actionIndex = 2;
        }
        if (args.length != actionIndex + 3 || !"get".equalsIgnoreCase(args[actionIndex])) {
            System.err.println("Usage: java -jar raft-dist/target/raft-1.0-SNAPSHOT.jar query [--json] get <target-port|target-host:port|id@target-host:port> <key>");
            System.exit(1);
        }

        Peer target = context.parsePeer().apply(args[actionIndex + 1]);
        String key = args[actionIndex + 2].trim();
        if (key.isBlank()) {
            System.err.println("Query key must not be blank");
            System.exit(1);
        }

        RaftTransportClient client = context.newClient("query-cli");
        try {
            ClientQueryRequest request = new ClientQueryRequest(
                    0L,
                    "query-cli",
                    StateMachineQuery.get(key).encode(),
                    context.authScheme().get(),
                    context.authToken().get()
            );
            ClientQueryResponse response = client.sendClientQueryRequest(target, request).get(5, TimeUnit.SECONDS);
            if ("REDIRECT".equals(response.getStatus()) && !response.getLeaderHost().isBlank() && response.getLeaderPort() > 0) {
                Peer leader = new Peer(response.getLeaderId(), new InetSocketAddress(response.getLeaderHost(), response.getLeaderPort()));
                response = client.sendClientQueryRequest(leader, request).get(5, TimeUnit.SECONDS);
            }
            System.out.println(json ? renderClientQueryResponseJson(response) : renderClientQueryResponse(response));
            if (!response.isSuccess()) {
                System.exit(2);
            }
        } catch (Exception e) {
            System.err.println("Query request failed: " + e.getMessage());
            System.exit(2);
        } finally {
            client.shutdown();
        }
    }

    public static String renderClientCommandResponse(String action, ClientCommandResponse response) {
        return "Command action=" + action
                + " status=" + response.getStatus()
                + " success=" + response.isSuccess()
                + " peer=" + response.getPeerId()
                + (response.getLeaderId() == null || response.getLeaderId().isBlank() ? "" : " leader=" + response.getLeaderId())
                + (response.getLeaderHost() == null || response.getLeaderHost().isBlank() || response.getLeaderPort() <= 0
                ? ""
                : " leaderEndpoint=" + response.getLeaderHost() + ":" + response.getLeaderPort())
                + " message=\"" + response.getMessage() + "\"";
    }

    public static String renderClientCommandResponseJson(String action, ClientCommandResponse response) {
        StringBuilder out = new StringBuilder();
        out.append("{\n");
        appendJsonField(out, "action", action, true, 1);
        appendJsonField(out, "status", response.getStatus(), true, 1);
        appendJsonField(out, "success", response.isSuccess(), true, 1);
        appendJsonField(out, "peerId", response.getPeerId(), true, 1);
        appendJsonField(out, "leaderId", response.getLeaderId(), true, 1);
        appendJsonField(out, "leaderHost", response.getLeaderHost(), true, 1);
        appendJsonField(out, "leaderPort", response.getLeaderPort(), true, 1);
        appendJsonField(out, "message", response.getMessage(), false, 1);
        out.append("}");
        return out.toString();
    }

    public static String renderClientQueryResponse(ClientQueryResponse response) {
        StringBuilder out = new StringBuilder();
        out.append("Query status=").append(response.getStatus())
                .append(" success=").append(response.isSuccess())
                .append(" peer=").append(response.getPeerId());
        if (response.getLeaderId() != null && !response.getLeaderId().isBlank()) {
            out.append(" leader=").append(response.getLeaderId());
        }
        if (response.getLeaderHost() != null && !response.getLeaderHost().isBlank() && response.getLeaderPort() > 0) {
            out.append(" leaderEndpoint=").append(response.getLeaderHost()).append(':').append(response.getLeaderPort());
        }
        out.append(" message=\"").append(response.getMessage()).append('"');
        if (response.isSuccess()) {
            StateMachineQueryResult.decode(response.getResult()).ifPresent(result -> {
                if (result.getType() == StateMachineQueryResult.Type.GET) {
                    out.append(" key=").append(result.getKey())
                            .append(" found=").append(result.isFound());
                    if (result.isFound()) {
                        out.append(" value=\"").append(result.getValue()).append('"');
                    }
                }
            });
        }
        return out.toString();
    }

    public static String renderClientQueryResponseJson(ClientQueryResponse response) {
        StringBuilder out = new StringBuilder();
        out.append("{\n");
        appendJsonField(out, "status", response.getStatus(), true, 1);
        appendJsonField(out, "success", response.isSuccess(), true, 1);
        appendJsonField(out, "peerId", response.getPeerId(), true, 1);
        appendJsonField(out, "leaderId", response.getLeaderId(), true, 1);
        appendJsonField(out, "leaderHost", response.getLeaderHost(), true, 1);
        appendJsonField(out, "leaderPort", response.getLeaderPort(), true, 1);
        appendJsonField(out, "message", response.getMessage(), true, 1);
        appendJsonQueryResult(out, response, false, 1);
        out.append("}");
        return out.toString();
    }

    private static void appendJsonQueryResult(StringBuilder out, ClientQueryResponse response, boolean comma, int indent) {
        indent(out, indent).append("\"result\": ");
        if (!response.isSuccess()) {
            out.append("null");
        } else {
            var decoded = StateMachineQueryResult.decode(response.getResult());
            if (decoded.isEmpty() || decoded.get().getType() != StateMachineQueryResult.Type.GET) {
                out.append("null");
            } else {
                StateMachineQueryResult result = decoded.get();
                out.append("{ ");
                out.append("\"type\": \"").append(escapeJson(result.getType().name())).append('"');
                out.append(", \"key\": \"").append(escapeJson(result.getKey())).append('"');
                out.append(", \"found\": ").append(result.isFound());
                if (result.isFound()) {
                    out.append(", \"value\": \"").append(escapeJson(result.getValue())).append('"');
                }
                out.append(" }");
            }
        }
        if (comma) {
            out.append(',');
        }
        out.append('\n');
    }

    private static void appendJsonField(StringBuilder out, String key, String value, boolean comma, int indent) {
        indent(out, indent).append('"').append(escapeJson(key)).append("\": ")
                .append('"').append(escapeJson(value == null ? "" : value)).append('"');
        if (comma) {
            out.append(',');
        }
        out.append('\n');
    }

    private static void appendJsonField(StringBuilder out, String key, boolean value, boolean comma, int indent) {
        indent(out, indent).append('"').append(escapeJson(key)).append("\": ").append(value);
        if (comma) {
            out.append(',');
        }
        out.append('\n');
    }

    private static void appendJsonField(StringBuilder out, String key, int value, boolean comma, int indent) {
        indent(out, indent).append('"').append(escapeJson(key)).append("\": ").append(value);
        if (comma) {
            out.append(',');
        }
        out.append('\n');
    }

    private static StringBuilder indent(StringBuilder out, int indent) {
        for (int i = 0; i < indent; i++) {
            out.append("  ");
        }
        return out;
    }

    private static String escapeJson(String value) {
        StringBuilder escaped = new StringBuilder();
        for (int i = 0; i < value.length(); i++) {
            char ch = value.charAt(i);
            switch (ch) {
                case '\\' -> escaped.append("\\\\");
                case '"' -> escaped.append("\\\"");
                case '\n' -> escaped.append("\\n");
                case '\r' -> escaped.append("\\r");
                case '\t' -> escaped.append("\\t");
                default -> escaped.append(ch);
            }
        }
        return escaped.toString();
    }
}
