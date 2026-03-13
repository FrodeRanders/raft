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

import java.io.PrintStream;

/**
 * Shared runtime module for operational cluster commands and telemetry.
 */
public final class OperationalApplicationModule implements RaftApplicationModule {
    @Override
    public boolean supportsCliCommand(String command) {
        return OperationalCliSupport.supportsCliCommand(command);
    }

    @Override
    public void printUsage(PrintStream err) {
        OperationalCliSupport.printUsage(err);
    }

    @Override
    public void runCli(String[] args, CliRuntimeContext context) {
        if (!OperationalCliSupport.handleIfSupported(args, context)) {
            throw new IllegalArgumentException("Unsupported operational CLI command: " + args[0]);
        }
    }
}
