package org.gautelis.raft.experiments.cpp;

import org.gautelis.raft.app.kv.KeyValueDemoAdapter;
import org.gautelis.raft.protocol.Peer;
import org.gautelis.raft.transport.netty.NettyTransportFactory;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

public final class JavaPeerMain {
    private JavaPeerMain() {
    }

    public static void main(String[] args) {
        if (args.length < 5) {
            usage();
            System.exit(1);
            return;
        }

        String peerId = args[0];
        String host = args[1];
        int port = Integer.parseInt(args[2]);
        long timeoutMillis = Long.parseLong(args[3]);
        String dataDir = args[4];

        Peer me = new Peer(peerId, new InetSocketAddress(host, port));
        List<Peer> peers = new ArrayList<>();
        Peer joinSeed = null;

        for (int i = 5; i < args.length; i++) {
            if ("--join-seed".equals(args[i])) {
                if (i + 1 >= args.length) {
                    System.err.println("Missing peer spec after --join-seed");
                    System.exit(1);
                    return;
                }
                joinSeed = parsePeer(args[++i]);
                continue;
            }
            Peer peer = parsePeer(args[i]);
            if (!peer.getId().equals(peerId)) {
                peers.add(peer);
            }
        }

        System.setProperty("raft.data.dir", dataDir);

        KeyValueDemoAdapter adapter = KeyValueDemoAdapter.builder(me)
                .withTimeoutMillis(timeoutMillis)
                .withPeers(peers)
                .withJoinSeed(joinSeed)
                .withTransportFactory(new NettyTransportFactory())
                .build();

        adapter.start();
    }

    private static Peer parsePeer(String spec) {
        int at = spec.indexOf('@');
        int colon = spec.lastIndexOf(':');
        if (at <= 0 || colon <= at + 1 || colon == spec.length() - 1) {
            throw new IllegalArgumentException("Invalid peer spec: " + spec);
        }
        String id = spec.substring(0, at);
        String host = spec.substring(at + 1, colon);
        int port = Integer.parseInt(spec.substring(colon + 1));
        return new Peer(id, new InetSocketAddress(host, port));
    }

    private static void usage() {
        System.err.println("Usage:");
        System.err.println("  JavaPeerMain <peer-id> <host> <port> <timeout-millis> <data-dir> [peer-id@host:port ...] [--join-seed <peer-id@host:port>]");
    }
}
