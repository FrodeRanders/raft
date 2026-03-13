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

import java.util.ArrayList;
import java.util.List;

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
            System.err.println("Usage:");
            System.err.println("  Peer specs may use an optional /learner or /voter suffix, for example id@host:port/learner");
            System.err.println("  java -jar raft-dist/target/raft-1.0-SNAPSHOT.jar <my-port|my-host:port|id@my-host:port[/role]> <peer-port|peer-host:port|id@peer-host:port[/role]> ...");
            System.err.println("  java -jar raft-dist/target/raft-1.0-SNAPSHOT.jar join <my-port|my-host:port|id@my-host:port[/role]> <seed-port|seed-host:port|id@seed-host:port[/role]>");
            for (RaftApplicationModule module : APPLICATION_MODULES) {
                module.printUsage(System.err);
            }
            System.exit(1);
        }

        for (RaftApplicationModule module : APPLICATION_MODULES) {
            if (module.supportsCliCommand(args[0])) {
                module.runCli(args, cliContext);
                return;
            }
        }

        boolean joinMode = "join".equalsIgnoreCase(args[0]);
        int offset = joinMode ? 1 : 0;
        if (args.length - offset < 2) {
            System.err.println("Not enough arguments for requested mode");
            System.exit(1);
        }

        Peer me = OperationalCliSupport.parsePeerSpec(args[offset], true);
        List<Peer> peers = new ArrayList<>();
        for (int i = offset + 1; i < args.length; i++) {
            peers.add(OperationalCliSupport.parsePeerSpec(args[i], false));
        }

        Peer joinSeed = joinMode ? peers.getFirst() : null;
        if (joinMode) {
            log.info("Starting Raft node {} in join mode via seed {}", me, joinSeed);
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
