package org.gautelis.raft.experiments.cpp;

import org.gautelis.raft.protocol.ClientCommandRequest;
import org.gautelis.raft.protocol.ClientCommandResponse;
import org.gautelis.raft.protocol.ClientQueryRequest;
import org.gautelis.raft.protocol.ClientQueryResponse;
import org.gautelis.raft.protocol.ClusterMemberSummary;
import org.gautelis.raft.protocol.ClusterSummaryRequest;
import org.gautelis.raft.protocol.ClusterSummaryResponse;
import org.gautelis.raft.protocol.AppendEntriesRequest;
import org.gautelis.raft.protocol.AppendEntriesResponse;
import org.gautelis.raft.protocol.InstallSnapshotRequest;
import org.gautelis.raft.protocol.InstallSnapshotResponse;
import org.gautelis.raft.protocol.Peer;
import org.gautelis.raft.protocol.StateMachineCommand;
import org.gautelis.raft.protocol.StateMachineCommandResult;
import org.gautelis.raft.protocol.StateMachineQuery;
import org.gautelis.raft.protocol.StateMachineQueryResult;
import org.gautelis.raft.protocol.TelemetryRequest;
import org.gautelis.raft.protocol.TelemetryResponse;
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
        if (args.length < 3) {
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
                case "client-put" -> runClientPut(client, target, args);
                case "client-get" -> runClientGet(client, target, args);
                case "telemetry" -> runTelemetry(client, target, args);
                case "cluster-summary" -> runClusterSummary(client, target, args);
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

    private static void runClientPut(RaftTransportClient client, Peer target, String[] args) throws Exception {
        if (args.length < 5) {
            usage();
            System.exit(1);
            return;
        }

        String key = args[3];
        String value = args[4];
        String peerId = args.length > 5 ? args[5] : "java-probe";

        ClientCommandRequest request = new ClientCommandRequest(0L, peerId, StateMachineCommand.put(key, value).encode());
        ClientCommandResponse response = client.sendClientCommandRequest(target, request).get(5, TimeUnit.SECONDS);
        printClientCommandResponse(response);
    }

    private static void runClientGet(RaftTransportClient client, Peer target, String[] args) throws Exception {
        if (args.length < 4) {
            usage();
            System.exit(1);
            return;
        }

        String key = args[3];
        String peerId = args.length > 4 ? args[4] : "java-probe";

        ClientQueryRequest request = new ClientQueryRequest(0L, peerId, StateMachineQuery.get(key).encode());
        ClientQueryResponse response = client.sendClientQueryRequest(target, request).get(5, TimeUnit.SECONDS);
        printClientQueryResponse(response);
    }

    private static void runTelemetry(RaftTransportClient client, Peer target, String[] args) throws Exception {
        boolean requireLeader = args.length > 3 && Boolean.parseBoolean(args[3]);

        TelemetryResponse response = client
                .sendTelemetryRequest(target, new TelemetryRequest(0L, "java-probe", false, requireLeader))
                .get(5, TimeUnit.SECONDS);

        System.out.println("peerId: " + response.getPeerId());
        System.out.println("status: " + response.getStatus());
        System.out.println("success: " + response.isSuccess());
        System.out.println("state: " + response.getState());
        System.out.println("leaderId: " + response.getLeaderId());
        System.out.println("term: " + response.getTerm());
        System.out.println("commitIndex: " + response.getCommitIndex());
        System.out.println("lastApplied: " + response.getLastApplied());
        System.out.println("lastLogIndex: " + response.getLastLogIndex());
        System.out.println("lastLogTerm: " + response.getLastLogTerm());
        System.out.println("snapshotIndex: " + response.getSnapshotIndex());
        System.out.println("snapshotTerm: " + response.getSnapshotTerm());
        System.out.println("jointConsensus: " + response.isJointConsensus());
        System.out.println("quorumAvailable: " + response.isQuorumAvailable());
        System.out.println("clusterHealth: " + response.getClusterHealth());
        for (ClusterMemberSummary member : response.getClusterMembers()) {
            System.out.println("member[" + member.peerId() + "]: local=" + member.local()
                    + " voting=" + member.voting()
                    + " role=" + member.role()
                    + " nextIndex=" + member.nextIndex()
                    + " matchIndex=" + member.matchIndex()
                    + " lag=" + member.lag());
        }
    }

    private static void runClusterSummary(RaftTransportClient client, Peer target, String[] args) throws Exception {
        ClusterSummaryResponse response = client
                .sendClusterSummaryRequest(target, new ClusterSummaryRequest(0L, "java-probe"))
                .get(5, TimeUnit.SECONDS);

        System.out.println("peerId: " + response.getPeerId());
        System.out.println("status: " + response.getStatus());
        System.out.println("success: " + response.isSuccess());
        System.out.println("state: " + response.getState());
        System.out.println("leaderId: " + response.getLeaderId());
        System.out.println("redirectLeaderId: " + response.getRedirectLeaderId());
        System.out.println("redirectLeaderHost: " + response.getRedirectLeaderHost());
        System.out.println("redirectLeaderPort: " + response.getRedirectLeaderPort());
        System.out.println("jointConsensus: " + response.isJointConsensus());
        System.out.println("quorumAvailable: " + response.isQuorumAvailable());
        System.out.println("clusterHealth: " + response.getClusterHealth());
        System.out.println("members: " + response.getMembers().size());
        for (ClusterMemberSummary member : response.getMembers()) {
            System.out.println("member[" + member.peerId() + "]: local=" + member.local()
                    + " voting=" + member.voting()
                    + " role=" + member.role()
                    + " nextIndex=" + member.nextIndex()
                    + " matchIndex=" + member.matchIndex()
                    + " lag=" + member.lag());
        }
    }

    private static void printClientCommandResponse(ClientCommandResponse response) {
        System.out.println("peerId: " + response.getPeerId());
        System.out.println("status: " + response.getStatus());
        System.out.println("success: " + response.isSuccess());
        System.out.println("leaderId: " + response.getLeaderId());
        System.out.println("leaderHost: " + response.getLeaderHost());
        System.out.println("leaderPort: " + response.getLeaderPort());
        System.out.println("message: " + response.getMessage());
        StateMachineCommandResult.decode(response.getResult()).ifPresent(result -> {
            if (result.getType() == StateMachineCommandResult.Type.CAS) {
                System.out.println("cas.matched: " + result.isMatched());
                System.out.println("cas.key: " + result.getKey());
                if (result.isCurrentPresent()) {
                    System.out.println("cas.currentValue: " + result.getCurrentValue());
                }
            }
        });
    }

    private static void printClientQueryResponse(ClientQueryResponse response) {
        System.out.println("peerId: " + response.getPeerId());
        System.out.println("status: " + response.getStatus());
        System.out.println("success: " + response.isSuccess());
        System.out.println("leaderId: " + response.getLeaderId());
        System.out.println("leaderHost: " + response.getLeaderHost());
        System.out.println("leaderPort: " + response.getLeaderPort());
        System.out.println("message: " + response.getMessage());
        StateMachineQueryResult.decode(response.getResult()).ifPresent(result -> {
            if (result.getType() == StateMachineQueryResult.Type.GET) {
                System.out.println("get.key: " + result.getKey());
                System.out.println("get.found: " + result.isFound());
                if (result.isFound()) {
                    System.out.println("get.value: " + result.getValue());
                }
            }
        });
    }

    private static void usage() {
        System.err.println("Usage:");
        System.err.println("  InteropProbe vote-request <host> <port> <candidate-id> <last-log-index> <last-log-term> [term]");
        System.err.println("  InteropProbe append-entries <host> <port> <leader-id> <prev-log-index> <prev-log-term> <leader-commit> [term]");
        System.err.println("  InteropProbe install-snapshot <host> <port> <leader-id> <last-included-index> <last-included-term> [term]");
        System.err.println("  InteropProbe client-put <host> <port> <key> <value> [peer-id]");
        System.err.println("  InteropProbe client-get <host> <port> <key> [peer-id]");
        System.err.println("  InteropProbe telemetry <host> <port> [require-leader-summary]");
        System.err.println("  InteropProbe cluster-summary <host> <port>");
    }
}
