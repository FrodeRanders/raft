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

import java.util.Comparator;
import java.util.List;

/**
 * Seed provider that turns DNS SRV answers into Raft peer endpoints.
 */
public final class DnsSrvSeedProvider implements SeedProvider {
    private final String serviceName;
    private final DnsSrvResolver resolver;

    public DnsSrvSeedProvider(String serviceName) {
        this(serviceName, new JndiDnsSrvResolver());
    }

    public DnsSrvSeedProvider(String serviceName, DnsSrvResolver resolver) {
        if (serviceName == null || serviceName.isBlank()) {
            throw new IllegalArgumentException("DNS SRV service name must not be blank");
        }
        if (resolver == null) {
            throw new IllegalArgumentException("resolver must not be null");
        }
        this.serviceName = serviceName.trim();
        this.resolver = resolver;
    }

    @Override
    public List<SeedEndpoint> seeds() {
        return resolver.resolve(serviceName).stream()
                .sorted(Comparator.comparingInt(DnsSrvResolver.SrvRecord::priority)
                        .thenComparingInt(DnsSrvResolver.SrvRecord::weight)
                        .thenComparing(DnsSrvResolver.SrvRecord::target))
                .map(record -> new SeedEndpoint(
                        SeedEndpoint.derivePeerId(record.target()),
                        record.target(),
                        record.port(),
                        org.gautelis.raft.protocol.Peer.Role.VOTER
                ))
                .toList();
    }
}
