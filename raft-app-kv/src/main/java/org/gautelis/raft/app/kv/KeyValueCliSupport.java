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
        if (args.length < 3) {
            System.err.println("Usage: java -jar target/raft.jar command <put|delete|clear> <target-port|target-host:port|id@target-host:port> [key] [value]");
            System.exit(1);
        }

        String action = args[1].trim().toLowerCase(Locale.ROOT);
        Peer target = context.parsePeer().apply(args[2]);
        byte[] command;
        switch (action) {
            case "put" -> {
                if (args.length != 5) {
                    System.err.println("Usage: java -jar target/raft.jar command put <target> <key> <value>");
                    System.exit(1);
                    return;
                }
                command = StateMachineCommand.put(args[3], args[4]).encode();
            }
            case "delete" -> {
                if (args.length != 4) {
                    System.err.println("Usage: java -jar target/raft.jar command delete <target> <key>");
                    System.exit(1);
                    return;
                }
                command = StateMachineCommand.delete(args[3]).encode();
            }
            case "clear" -> {
                if (args.length != 3) {
                    System.err.println("Usage: java -jar target/raft.jar command clear <target>");
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
            System.out.println(renderClientCommandResponse(action, response));
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
        if (args.length != 4 || !"get".equalsIgnoreCase(args[1])) {
            System.err.println("Usage: java -jar target/raft.jar query get <target-port|target-host:port|id@target-host:port> <key>");
            System.exit(1);
        }

        Peer target = context.parsePeer().apply(args[2]);
        String key = args[3].trim();
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
            System.out.println(renderClientQueryResponse(response));
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
}
