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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.gautelis.raft.app.kv.KeyValueApplicationModule;
import org.gautelis.raft.app.reference.ReferenceDataApplicationModule;
import org.gautelis.raft.protocol.Peer;
import org.gautelis.raft.transport.netty.NettyTransportFactory;
import org.gautelis.raft.transport.netty.RaftClient;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Provides the runnable demo node process and the small CLI for admin, query, and telemetry actions.
 */
public class Application {
    private static final Logger log = LogManager.getLogger(Application.class);
    private static final List<RaftApplicationModule> APPLICATION_MODULES = List.of(
            new OperationalApplicationModule(),
            new KeyValueApplicationModule(),
            new ReferenceDataApplicationModule()
    );

    public static void main(String[] args) {
        RuntimeConfiguration configuration = RuntimeConfiguration.load();
        CliRuntimeContext cliContext = new CliRuntimeContext(
                spec -> OperationalCliSupport.parsePeerSpec(spec, false),
                clientId -> new RaftClient(clientId, null),
                configuration::requestAuthScheme,
                configuration::requestAuthToken
        );
        if (args.length < 2) {
            if (!configuration.hasClusterSrv() && !configuration.hasNodeId()) {
                printUsageAndExit();
            }
        }

        for (RaftApplicationModule module : APPLICATION_MODULES) {
            if (args.length > 0 && module.supportsCliCommand(args[0])) {
                module.runCli(args, cliContext);
                return;
            }
        }

        Startup startup = parseStartup(args, configuration);
        Peer me = startup.me();
        List<Peer> peers = startup.peers();
        Peer joinSeed = startup.joinSeed();
        if (startup.joinMode()) {
            log.info("Starting Raft node {} in join mode via seed {}", me, joinSeed);
        } else if (startup.clusterSrv() != null && !startup.clusterSrv().isBlank()) {
            log.info("Starting Raft node {} with {} DNS SRV seed peers from {}", me, peers.size(), startup.clusterSrv());
        } else {
            log.info("Starting Raft cluster with {} initial members; runtime configuration changes are supported through replicated config commands", peers.size() + 1);
        }

        long timeoutMillis = 2000;

        OperationalCliSupport.startDemoTelemetryLoop(
                me,
                clientId -> new RaftClient(clientId, null),
                configuration::requestAuthScheme,
                configuration::requestAuthToken,
                configuration
        );
        BasicAdapter adapter = createAdapter(timeoutMillis, me, peers, joinSeed, configuration);
        adapter.start();
    }

    private static Startup parseStartup(String[] args, RuntimeConfiguration configuration) {
        if (usesOptionStartup(args, configuration)) {
            return parseOptionStartup(args, configuration);
        }
        return parseLegacyStartup(args);
    }

    private static boolean usesOptionStartup(String[] args, RuntimeConfiguration configuration) {
        if (configuration.hasClusterSrv() || configuration.hasNodeId()) {
            return true;
        }
        for (String arg : args) {
            if (arg.startsWith("--")) {
                return true;
            }
        }
        return false;
    }

    private static Startup parseLegacyStartup(String[] args) {
        boolean joinMode = args.length > 0 && "join".equalsIgnoreCase(args[0]);
        int offset = joinMode ? 1 : 0;
        if (args.length - offset < 2) {
            System.err.println("Not enough arguments for requested mode");
            printUsageAndExit();
        }

        Peer me = OperationalCliSupport.parsePeerSpec(args[offset], true);
        List<Peer> peers = new ArrayList<>();
        for (int i = offset + 1; i < args.length; i++) {
            peers.add(OperationalCliSupport.parsePeerSpec(args[i], false));
        }
        Peer joinSeed = joinMode ? peers.getFirst() : null;
        return new Startup(me, List.copyOf(peers), joinSeed, joinMode, "");
    }

    private static Startup parseOptionStartup(String[] args, RuntimeConfiguration configuration) {
        Map<String, List<String>> options = parseOptions(args);
        String nodeId = firstOption(options, "node-id", configuration.nodeId());
        if (nodeId == null || nodeId.isBlank()) {
            throw new IllegalArgumentException("Missing node id; set --node-id or RAFT_NODE_ID");
        }

        Endpoint bind = parseBindEndpoint(firstOption(options, "bind", null), configuration);
        String advertisedHost = firstOption(options, "advertise-host", configuration.advertiseHost());
        int advertisedPort = parsePort(firstOption(options, "advertise-port", null), configuration.advertisePort());
        Endpoint advertised = parseAdvertisedEndpoint(
                firstOption(options, "advertise", null),
                nodeId,
                bind,
                advertisedHost,
                advertisedPort
        );
        Peer me = new Peer(nodeId, new InetSocketAddress(advertised.host(), advertised.port()));

        List<Peer> peers = new ArrayList<>();
        for (String peerSpec : options.getOrDefault("peer", List.of())) {
            peers.add(OperationalCliSupport.parsePeerSpec(peerSpec, false));
        }

        String clusterSrv = firstOption(options, "cluster-srv", configuration.clusterSrv());
        if (clusterSrv != null && !clusterSrv.isBlank()) {
            List<Peer> dnsPeers = new DnsSrvSeedProvider(clusterSrv).seeds().stream()
                    .map(SeedEndpoint::toPeer)
                    .filter(peer -> !nodeId.equals(peer.getId()))
                    .toList();
            peers.addAll(dnsPeers);
        }
        peers = deduplicatePeers(peers);

        boolean bootstrapNewCluster = booleanOption(options, "bootstrap-new-cluster", configuration.bootstrapNewCluster());
        if (peers.isEmpty() && !bootstrapNewCluster) {
            throw new IllegalStateException("No seed peers resolved; refusing to bootstrap without --bootstrap-new-cluster=true");
        }
        return new Startup(me, peers, null, false, clusterSrv == null ? "" : clusterSrv);
    }

    private static Map<String, List<String>> parseOptions(String[] args) {
        Map<String, List<String>> options = new LinkedHashMap<>();
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            if (!arg.startsWith("--")) {
                throw new IllegalArgumentException("Unexpected positional argument in option startup mode: " + arg);
            }
            String option = arg.substring(2);
            String value = "true";
            int equals = option.indexOf('=');
            if (equals >= 0) {
                value = option.substring(equals + 1);
                option = option.substring(0, equals);
            } else if (i + 1 < args.length && !args[i + 1].startsWith("--")) {
                value = args[++i];
            }
            options.computeIfAbsent(option, ignored -> new ArrayList<>()).add(value);
        }
        return options;
    }

    private static String firstOption(Map<String, List<String>> options, String name, String fallback) {
        List<String> values = options.get(name);
        if (values == null || values.isEmpty()) {
            return fallback;
        }
        return values.getLast();
    }

    private static boolean booleanOption(Map<String, List<String>> options, String name, boolean fallback) {
        String value = firstOption(options, name, null);
        if (value == null || value.isBlank()) {
            return fallback;
        }
        return switch (value.trim().toLowerCase(java.util.Locale.ROOT)) {
            case "true", "1", "yes", "on" -> true;
            case "false", "0", "no", "off" -> false;
            default -> throw new IllegalArgumentException("Invalid boolean value for --" + name + ": " + value);
        };
    }

    private static Endpoint parseBindEndpoint(String bind, RuntimeConfiguration configuration) {
        if (bind != null && !bind.isBlank()) {
            return parseEndpoint(bind, "bind");
        }
        if (configuration.bindPort() <= 0) {
            throw new IllegalArgumentException("Missing bind port; set --bind or RAFT_BIND_PORT");
        }
        return new Endpoint(configuration.bindHost(), configuration.bindPort());
    }

    private static Endpoint parseAdvertisedEndpoint(String advertised, String nodeId, Endpoint bind, String advertisedHost, int advertisedPort) {
        if (advertised != null && !advertised.isBlank()) {
            return parseEndpoint(advertised, "advertise");
        }
        String host = advertisedHost == null || advertisedHost.isBlank()
                ? ("0.0.0.0".equals(bind.host()) || "::".equals(bind.host()) ? nodeId : bind.host())
                : advertisedHost;
        int port = advertisedPort > 0 ? advertisedPort : bind.port();
        return new Endpoint(host, port);
    }

    private static Endpoint parseEndpoint(String value, String fieldName) {
        int colon = value.lastIndexOf(':');
        if (colon <= 0 || colon == value.length() - 1) {
            throw new IllegalArgumentException("Invalid " + fieldName + " endpoint, expected host:port: " + value);
        }
        return new Endpoint(value.substring(0, colon), Integer.parseInt(value.substring(colon + 1)));
    }

    private static int parsePort(String value, int fallback) {
        if (value == null || value.isBlank()) {
            return fallback;
        }
        return Integer.parseInt(value);
    }

    private static List<Peer> deduplicatePeers(List<Peer> peers) {
        Map<String, Peer> byId = new LinkedHashMap<>();
        for (Peer peer : peers) {
            Peer previous = byId.putIfAbsent(peer.getId(), peer);
            if (previous != null && !previous.equals(peer)) {
                throw new IllegalArgumentException("Conflicting seed peer definitions for " + peer.getId() + ": " + previous + " vs " + peer);
            }
        }
        return List.copyOf(byId.values());
    }

    private static void printUsageAndExit() {
        System.err.println("Usage:");
        System.err.println("  Peer specs may use an optional /learner or /voter suffix, for example id@host:port/learner");
        System.err.println("  java -jar raft-dist/target/raft-1.0-SNAPSHOT.jar <my-port|my-host:port|id@my-host:port[/role]> <peer-port|peer-host:port|id@peer-host:port[/role]> ...");
        System.err.println("  java -jar raft-dist/target/raft-1.0-SNAPSHOT.jar join <my-port|my-host:port|id@my-host:port[/role]> <seed-port|seed-host:port|id@seed-host:port[/role]>");
        System.err.println("  java -jar raft-dist/target/raft-1.0-SNAPSHOT.jar --node-id <id> --bind <host:port> [--advertise <host:port>] [--cluster-srv <name>] [--peer <id@host:port>]... [--bootstrap-new-cluster=true]");
        for (RaftApplicationModule module : APPLICATION_MODULES) {
            module.printUsage(System.err);
        }
        System.exit(1);
    }

    private record Endpoint(String host, int port) {
    }

    private record Startup(Peer me, List<Peer> peers, Peer joinSeed, boolean joinMode, String clusterSrv) {
    }

    private static BasicAdapter createAdapter(long timeoutMillis, Peer me, List<Peer> peers, Peer joinSeed, RuntimeConfiguration configuration) {
        String mode = configuration.adapterMode();
        RuntimeAdapterContext runtimeContext = new RuntimeAdapterContext(
                configuration,
                createCommandAuthenticator(configuration),
                new NettyTransportFactory()
        );
        RaftApplicationFactory factory = APPLICATION_MODULES.stream()
                .filter(module -> module.supportsRuntimeMode(mode))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Unknown adapter mode: " + mode))
                .createFactory(runtimeContext);
        return factory.create(timeoutMillis, me, peers, joinSeed, configuration);
    }

    private static ClientCommandAuthenticator createCommandAuthenticator(RuntimeConfiguration configuration) {
        String mode = configuration.authMode();
        return switch (mode) {
            case "none" -> ClientCommandAuthenticator.none();
            case "shared-secret" -> new SharedSecretCommandAuthenticator(configuration.requireSharedSecret());
            default -> throw new IllegalArgumentException("Unknown command auth mode: " + mode);
        };
    }
}
