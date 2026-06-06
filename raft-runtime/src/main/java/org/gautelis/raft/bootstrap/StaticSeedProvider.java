/*
 * Copyright (C) 2026 Frode Randers
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

import java.util.List;

/**
 * Seed provider for the existing explicit peer-spec startup mode.
 */
public final class StaticSeedProvider implements SeedProvider {
    private final List<SeedEndpoint> seeds;

    public StaticSeedProvider(List<Peer> peers) {
        this.seeds = peers == null
                ? List.of()
                : peers.stream()
                .filter(peer -> peer != null && peer.getAddress() != null)
                .map(peer -> new SeedEndpoint(
                        peer.getId(),
                        peer.getAddress().getHostString(),
                        peer.getAddress().getPort(),
                        peer.getRole()
                ))
                .toList();
    }

    @Override
    public List<SeedEndpoint> seeds() {
        return seeds;
    }
}
