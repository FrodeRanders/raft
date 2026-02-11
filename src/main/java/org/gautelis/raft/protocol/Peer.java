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
package org.gautelis.raft.protocol;

import java.net.InetSocketAddress;
import java.util.Objects;

public class Peer {
    private String id;
    private InetSocketAddress address;
    public Peer(String id, InetSocketAddress address) {
        if (id == null || id.isBlank()) {
            throw new IllegalArgumentException("Peer id must be non-null and non-blank");
        }
        this.id = id;
        this.address = address;
    }

    public String getId() { return id; }
    public InetSocketAddress getAddress() { return address; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Peer peer)) return false;
        return Objects.equals(id, peer.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("Peer{");
        sb.append("id=").append(id);
        sb.append(", address=").append(address);
        sb.append('}');
        return sb.toString();
    }
}
