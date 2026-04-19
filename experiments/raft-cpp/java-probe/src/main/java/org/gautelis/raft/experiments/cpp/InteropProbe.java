package org.gautelis.raft.experiments.cpp;

import org.gautelis.raft.protocol.AppendEntriesRequest;
import org.gautelis.raft.protocol.AppendEntriesResponse;
import org.gautelis.raft.protocol.InstallSnapshotRequest;
import org.gautelis.raft.protocol.InstallSnapshotResponse;
import org.gautelis.raft.protocol.Peer;
import org.gautelis.raft.protocol.VoteRequest;
import org.gautelis.raft.protocol.VoteResponse;
import org.gautelis.raft.transport.RaftTransportClient;
import org.gautelis.raft.transport.netty.NettyTransportFactory;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.TimeUnit;

public final class InteropProbe {
    private InteropProbe() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            usage();
            System.exit(1);
            return;
        }

        String command = args[0];
        String host = args[1];
        int port = Integer.parseInt(args[2]);
        Peer target = new Peer("cpp-target", new InetSocketAddress(host, port));

        NettyTransportFactory factory = new NettyTransportFactory();
        RaftTransportClient client = factory.createClient("java-probe", null);
        try {
            switch (command) {
                case "vote-request" -> runVoteRequest(client, target, args);
                case "append-entries" -> runAppendEntries(client, target, args);
                case "install-snapshot" -> runInstallSnapshot(client, target, args);
                default -> {
                    usage();
                    System.exit(1);
                }
            }
        } finally {
            client.shutdown();
        }
    }

    private static void runVoteRequest(RaftTransportClient client, Peer target, String[] args) throws Exception {
        if (args.length < 6) {
            usage();
            System.exit(1);
            return;
        }

        String candidateId = args[3];
        long lastLogIndex = Long.parseLong(args[4]);
        long lastLogTerm = Long.parseLong(args[5]);
        long term = args.length > 6 ? Long.parseLong(args[6]) : 0L;

        VoteResponse response = client
                .requestVoteFromAll(List.of(target), new VoteRequest(term, candidateId, lastLogIndex, lastLogTerm))
                .get(5, TimeUnit.SECONDS)
                .getFirst();

        System.out.println("peerId: " + response.getPeerId());
        System.out.println("term: " + response.getTerm());
        System.out.println("currentTerm: " + response.getCurrentTerm());
        System.out.println("voteGranted: " + response.isVoteGranted());
    }

    private static void runAppendEntries(RaftTransportClient client, Peer target, String[] args) throws Exception {
        if (args.length < 7) {
            usage();
            System.exit(1);
            return;
        }

        String leaderId = args[3];
        long prevLogIndex = Long.parseLong(args[4]);
        long prevLogTerm = Long.parseLong(args[5]);
        long leaderCommit = Long.parseLong(args[6]);
        long term = args.length > 7 ? Long.parseLong(args[7]) : 0L;

        AppendEntriesResponse response = client
                .sendAppendEntries(target, new AppendEntriesRequest(term, leaderId, prevLogIndex, prevLogTerm, leaderCommit, List.of()))
                .get(5, TimeUnit.SECONDS);

        System.out.println("peerId: " + response.getPeerId());
        System.out.println("term: " + response.getTerm());
        System.out.println("success: " + response.isSuccess());
        System.out.println("matchIndex: " + response.getMatchIndex());
    }

    private static void runInstallSnapshot(RaftTransportClient client, Peer target, String[] args) throws Exception {
        if (args.length < 6) {
            usage();
            System.exit(1);
            return;
        }

        String leaderId = args[3];
        long lastIncludedIndex = Long.parseLong(args[4]);
        long lastIncludedTerm = Long.parseLong(args[5]);
        long term = args.length > 6 ? Long.parseLong(args[6]) : 0L;

        InstallSnapshotResponse response = client
                .sendInstallSnapshot(target, new InstallSnapshotRequest(term, leaderId, lastIncludedIndex, lastIncludedTerm, 0L, new byte[0], true))
                .get(5, TimeUnit.SECONDS);

        System.out.println("peerId: " + response.getPeerId());
        System.out.println("term: " + response.getTerm());
        System.out.println("success: " + response.isSuccess());
        System.out.println("lastIncludedIndex: " + response.getLastIncludedIndex());
    }

    private static void usage() {
        System.err.println("Usage:");
        System.err.println("  InteropProbe vote-request <host> <port> <candidate-id> <last-log-index> <last-log-term> [term]");
        System.err.println("  InteropProbe append-entries <host> <port> <leader-id> <prev-log-index> <prev-log-term> <leader-commit> [term]");
        System.err.println("  InteropProbe install-snapshot <host> <port> <leader-id> <last-included-index> <last-included-term> [term]");
    }
}
