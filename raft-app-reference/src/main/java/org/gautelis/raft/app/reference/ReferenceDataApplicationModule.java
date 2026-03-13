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
import org.gautelis.raft.bootstrap.RaftApplicationFactory;
import org.gautelis.raft.bootstrap.RaftApplicationModule;
import org.gautelis.raft.bootstrap.RuntimeAdapterContext;

import java.io.PrintStream;

/**
 * Application module for the reference-data app.
 */
public final class ReferenceDataApplicationModule implements RaftApplicationModule {
    @Override
    public boolean supportsRuntimeMode(String mode) {
        return "reference-data".equals(mode) || "reference".equals(mode) || "refdata".equals(mode);
    }

    @Override
    public boolean supportsCliCommand(String command) {
        return "reference-data".equalsIgnoreCase(command);
    }

    @Override
    public void printUsage(PrintStream err) {
        err.println("  java -jar raft-dist/target/raft-1.0-SNAPSHOT.jar reference-data <upsert-product|remove-product|upsert-variant|remove-variant|variants> <target> ...");
    }

    @Override
    public void runCli(String[] args, CliRuntimeContext context) {
        ReferenceDataCliSupport.run(args, context);
    }

    @Override
    public RaftApplicationFactory createFactory(RuntimeAdapterContext context) {
        return new ReferenceDataApplicationFactory(context.commandAllowList(), context.authenticator(), context.transportFactory());
    }
}
