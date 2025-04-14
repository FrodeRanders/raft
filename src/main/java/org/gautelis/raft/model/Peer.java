package org.gautelis.raft.model;

import java.net.InetSocketAddress;

public class Peer {
    private String id;
    private InetSocketAddress address;

    // Default constructor needed for Jackson
    public Peer() {}

    public Peer(String id, InetSocketAddress address) {
        this.id = id;
        this.address = address;
    }

    public String getId() { return id; }
    public InetSocketAddress getAddress() { return address; }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("Peer{");
        sb.append("id=").append(id);
        sb.append(", address=").append(address);
        sb.append('}');
        return sb.toString();
    }
}
