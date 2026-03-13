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
    default boolean supportsRuntimeMode(String mode) {
        return false;
    }

    boolean supportsCliCommand(String command);

    void printUsage(PrintStream err);

    void runCli(String[] args, CliRuntimeContext context);

    default RaftApplicationFactory createFactory(RuntimeAdapterContext context) {
        throw new UnsupportedOperationException("Module does not provide a runtime adapter factory");
    }
}
