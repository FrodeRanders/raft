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

import org.gautelis.raft.bootstrap.CliRuntimeContext;
import org.gautelis.raft.bootstrap.RaftApplicationFactory;
import org.gautelis.raft.bootstrap.RaftApplicationModule;
import org.gautelis.raft.bootstrap.RuntimeAdapterContext;

import java.io.PrintStream;

/**
 * Application module for the key-value demo.
 */
public final class KeyValueApplicationModule implements RaftApplicationModule {
    @Override
    public boolean supportsRuntimeMode(String mode) {
        return "basic".equals(mode);
    }

    @Override
    public boolean supportsCliCommand(String command) {
        return "command".equalsIgnoreCase(command) || "query".equalsIgnoreCase(command);
    }

    @Override
    public void printUsage(PrintStream err) {
        err.println("  java -jar target/raft.jar command <put|delete|clear> <target-port|target-host:port|id@target-host:port[/role]> [key] [value]");
        err.println("  java -jar target/raft.jar query get <target-port|target-host:port|id@target-host:port[/role]> <key>");
    }

    @Override
    public void runCli(String[] args, CliRuntimeContext context) {
        if ("command".equalsIgnoreCase(args[0])) {
            KeyValueCliSupport.runCommand(args, context);
            return;
        }
        if ("query".equalsIgnoreCase(args[0])) {
            KeyValueCliSupport.runQuery(args, context);
            return;
        }
        throw new IllegalArgumentException("Unsupported key-value CLI command: " + args[0]);
    }

    @Override
    public RaftApplicationFactory createFactory(RuntimeAdapterContext context) {
        return new KeyValueApplicationFactory(context.commandAllowList(), context.authenticator(), context.authMode(), context.transportFactory());
    }
}
