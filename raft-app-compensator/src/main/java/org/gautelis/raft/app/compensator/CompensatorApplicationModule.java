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
package org.gautelis.raft.app.compensator;

import org.gautelis.raft.bootstrap.CliRuntimeContext;
import org.gautelis.raft.bootstrap.RaftApplicationFactory;
import org.gautelis.raft.bootstrap.RaftApplicationModule;
import org.gautelis.raft.bootstrap.RuntimeAdapterContext;

import java.io.PrintStream;

/**
 * Application module for the compensator work-partitioning demo.
 *
 * <p>Use runtime mode {@code compensator} to run a compensator node.</p>
 */
public final class CompensatorApplicationModule implements RaftApplicationModule {

    @Override
    public boolean supportsRuntimeMode(String mode) {
        return "compensator".equals(mode);
    }

    @Override
    public boolean supportsCliCommand(String command) {
        return "compensator-status".equalsIgnoreCase(command);
    }

    @Override
    public void printUsage(PrintStream err) {
        err.println("  java -jar raft-dist/target/raft-1.0-SNAPSHOT.jar compensator-status <target-port|target-host:port|id@target-host:port[/role]>");
    }

    @Override
    public void runCli(String[] args, CliRuntimeContext context) {
        if ("compensator-status".equalsIgnoreCase(args[0])) {
            CompensatorCliSupport.runStatus(args, context);
            return;
        }
        throw new IllegalArgumentException("Unsupported compensator CLI command: " + args[0]);
    }

    @Override
    public RaftApplicationFactory createFactory(RuntimeAdapterContext context) {
        return new CompensatorApplicationFactory();
    }
}
