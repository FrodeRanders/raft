package org.gautelis.raft.utilities;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.gautelis.raft.RaftClient;
import org.gautelis.raft.RaftServer;
import org.gautelis.raft.RaftStateMachine;
import org.gautelis.raft.model.Peer;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

public class Application {
    private static final Logger log = LogManager.getLogger(Application.class);

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: java -jar target/raft-application.jar <my-port> <peer-port> ...");
            System.exit(1);
        }

        int port = Integer.parseInt(args[0]);
        Peer me = new Peer("server-" + port, new InetSocketAddress("localhost", port));

        List<Peer> peers = new ArrayList<>();
        for (int i = 1; i < args.length; i++) {
            int peerPort = Integer.parseInt(args[i]);
            peers.add(new Peer("server-" + peerPort, new InetSocketAddress("localhost", peerPort)));
        }

        RaftClient client = new RaftClient();
        long timeoutMillis = 5000 + Math.round(2000.0 * Math.random());
        RaftStateMachine stateMachine = new RaftStateMachine(me, peers, timeoutMillis, client);

        try {
            RaftServer raftServer = new RaftServer(stateMachine, port);

            log.info("Starting server on port {}...", port);
            raftServer.start();
        }
        catch (InterruptedException ie) {
            log.info("Interrupted!", ie);
        }
    }
}
