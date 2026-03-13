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

import org.gautelis.raft.RaftNode;
import org.gautelis.raft.protocol.Peer;
import org.slf4j.Logger;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

record AdapterHandlerContext(
        Logger log,
        Peer me,
        RuntimeConfiguration runtimeConfiguration,
        Supplier<RaftNode> stateMachineSupplier,
        Function<AdapterRequestAuth, ClientCommandAuthenticationResult> authenticator,
        Predicate<byte[]> commandValidator,
        Predicate<byte[]> queryValidator,
        Function<byte[], Boolean> commandSubmitter,
        Supplier<ClientWriteAdmissionPolicy> writeAdmissionPolicy,
        Supplier<ClientCommandAuthorizer> commandAuthorizer,
        Supplier<ClientCommandAuthenticator> commandAuthenticator,
        Consumer<Peer> peerRegistrar
) {
}
