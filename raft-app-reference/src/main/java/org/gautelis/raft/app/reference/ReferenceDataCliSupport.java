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
package org.gautelis.raft.app.reference;

import org.gautelis.raft.bootstrap.CliRuntimeContext;
import org.gautelis.raft.protocol.ClientCommandRequest;
import org.gautelis.raft.protocol.ClientCommandResponse;
import org.gautelis.raft.protocol.ClientQueryRequest;
import org.gautelis.raft.protocol.ClientQueryResponse;
import org.gautelis.raft.protocol.Peer;
import org.gautelis.raft.transport.RaftTransportClient;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

/**
 * CLI helpers for the reference-data application.
 */
public final class ReferenceDataCliSupport {
    private ReferenceDataCliSupport() {
    }

    public static void run(String[] args,
                           CliRuntimeContext context) {
        if (args.length < 4) {
            usageAndExit();
        }

        String action = args[1].trim().toLowerCase(java.util.Locale.ROOT);
        Peer target = context.parsePeer().apply(args[2]);
        switch (action) {
            case "upsert-product" -> sendCommand(
                    target,
                    ReferenceDataCommand.upsertProduct(require(args, 3, "product-id"), require(args, 4, "product-name")).encode(),
                    "upsert-product",
                    context
            );
            case "remove-product" -> sendCommand(
                    target,
                    ReferenceDataCommand.removeProduct(require(args, 3, "product-id")).encode(),
                    "remove-product",
                    context
            );
            case "upsert-variant" -> sendCommand(
                    target,
                    ReferenceDataCommand.upsertVariant(
                            require(args, 3, "product-id"),
                            require(args, 4, "variant-id"),
                            require(args, 5, "variant-name")
                    ).encode(),
                    "upsert-variant",
                    context
            );
            case "remove-variant" -> sendCommand(
                    target,
                    ReferenceDataCommand.removeVariant(
                            require(args, 3, "product-id"),
                            require(args, 4, "variant-id")
                    ).encode(),
                    "remove-variant",
                    context
            );
            case "variants" -> sendQuery(
                    target,
                    ReferenceDataQuery.variantsForProduct(require(args, 3, "product-id")).encode(),
                    context
            );
            default -> usageAndExit();
        }
    }

    private static void sendCommand(Peer target, byte[] payload, String action, CliRuntimeContext context) {
        RaftTransportClient client = context.newClient("reference-data-cli");
        try {
            ClientCommandRequest request = new ClientCommandRequest(0L, "reference-data-cli", payload, context.authScheme().get(), context.authToken().get());
            ClientCommandResponse response = client.sendClientCommandRequest(target, request).get(5, TimeUnit.SECONDS);
            if ("REDIRECT".equals(response.getStatus()) && !response.getLeaderHost().isBlank() && response.getLeaderPort() > 0) {
                Peer leader = new Peer(response.getLeaderId(), new InetSocketAddress(response.getLeaderHost(), response.getLeaderPort()));
                response = client.sendClientCommandRequest(leader, request).get(5, TimeUnit.SECONDS);
            }
            System.out.println(renderCommandResponse(action, response));
            if (!response.isSuccess()) {
                System.exit(2);
            }
        } catch (Exception e) {
            System.err.println("Reference-data command failed: " + e.getMessage());
            System.exit(2);
        } finally {
            client.shutdown();
        }
    }

    private static void sendQuery(Peer target, byte[] payload, CliRuntimeContext context) {
        RaftTransportClient client = context.newClient("reference-data-query-cli");
        try {
            ClientQueryRequest request = new ClientQueryRequest(0L, "reference-data-query-cli", payload, context.authScheme().get(), context.authToken().get());
            ClientQueryResponse response = client.sendClientQueryRequest(target, request).get(5, TimeUnit.SECONDS);
            if ("REDIRECT".equals(response.getStatus()) && !response.getLeaderHost().isBlank() && response.getLeaderPort() > 0) {
                Peer leader = new Peer(response.getLeaderId(), new InetSocketAddress(response.getLeaderHost(), response.getLeaderPort()));
                response = client.sendClientQueryRequest(leader, request).get(5, TimeUnit.SECONDS);
            }
            System.out.println(renderQueryResponse(response));
            if (!response.isSuccess()) {
                System.exit(2);
            }
        } catch (Exception e) {
            System.err.println("Reference-data query failed: " + e.getMessage());
            System.exit(2);
        } finally {
            client.shutdown();
        }
    }

    public static String renderCommandResponse(String action, ClientCommandResponse response) {
        return "ReferenceData action=" + action
                + " status=" + response.getStatus()
                + " success=" + response.isSuccess()
                + " peer=" + response.getPeerId()
                + (response.getLeaderId() == null || response.getLeaderId().isBlank() ? "" : " leader=" + response.getLeaderId())
                + " message=\"" + response.getMessage() + "\"";
    }

    public static String renderQueryResponse(ClientQueryResponse response) {
        StringBuilder out = new StringBuilder();
        out.append("ReferenceDataQuery status=").append(response.getStatus())
                .append(" success=").append(response.isSuccess())
                .append(" peer=").append(response.getPeerId());
        if (response.getLeaderId() != null && !response.getLeaderId().isBlank()) {
            out.append(" leader=").append(response.getLeaderId());
        }
        out.append(" message=\"").append(response.getMessage()).append('"');
        if (response.isSuccess()) {
            ReferenceDataQueryResult.decode(response.getResult()).ifPresent(result -> {
                out.append(" product=").append(result.productId())
                        .append(" found=").append(result.found());
                if (result.found()) {
                    out.append(" name=\"").append(result.productName()).append('"')
                            .append(" variants=").append(result.variants().size());
                }
            });
        }
        return out.toString();
    }

    private static String require(String[] args, int index, String label) {
        if (args.length <= index || args[index].isBlank()) {
            System.err.println("Missing required argument: " + label);
            System.exit(1);
        }
        return args[index].trim();
    }

    private static void usageAndExit() {
        System.err.println("Usage:");
        System.err.println("  java -jar raft-dist/target/raft-1.0-SNAPSHOT.jar reference-data upsert-product <target> <product-id> <product-name>");
        System.err.println("  java -jar raft-dist/target/raft-1.0-SNAPSHOT.jar reference-data remove-product <target> <product-id>");
        System.err.println("  java -jar raft-dist/target/raft-1.0-SNAPSHOT.jar reference-data upsert-variant <target> <product-id> <variant-id> <variant-name>");
        System.err.println("  java -jar raft-dist/target/raft-1.0-SNAPSHOT.jar reference-data remove-variant <target> <product-id> <variant-id>");
        System.err.println("  java -jar raft-dist/target/raft-1.0-SNAPSHOT.jar reference-data variants <target> <product-id>");
        System.exit(1);
    }
}
