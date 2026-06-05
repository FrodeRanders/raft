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

import org.gautelis.raft.protocol.Peer;

import java.io.PrintStream;

/**
 * Describes one application package that plugs into the shared runtime.
 */
public interface RaftApplicationModule {
    /**
     * Reports whether this module can provide the runtime adapter for a named mode.
     *
     * @param mode runtime mode, for example {@code basic} or {@code reference-data}
     * @return {@code true} when this module supports the mode
     */
    default boolean supportsRuntimeMode(String mode) {
        return false;
    }

    /**
     * Reports whether this module owns a command-line command.
     *
     * @param command first CLI token
     * @return {@code true} when this module can execute the command
     */
    boolean supportsCliCommand(String command);

    /**
     * Prints application-specific command usage.
     *
     * @param err output stream for usage text
     */
    void printUsage(PrintStream err);

    /**
     * Runs an application-specific CLI command.
     *
     * @param args command-line arguments
     * @param context shared CLI runtime context
     */
    void runCli(String[] args, CliRuntimeContext context);

    /**
     * Creates the runtime adapter factory for this application module.
     *
     * @param context runtime adapter context with shared services and policies
     * @return application factory
     */
    default RaftApplicationFactory createFactory(RuntimeAdapterContext context) {
        throw new UnsupportedOperationException("Module does not provide a runtime adapter factory");
    }
}
