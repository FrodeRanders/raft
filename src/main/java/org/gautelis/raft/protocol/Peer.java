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
