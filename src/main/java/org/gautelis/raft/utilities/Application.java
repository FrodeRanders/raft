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
package org.gautelis.raft.utilities;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.gautelis.raft.model.Peer;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

public class Application {
    private static final Logger log = LogManager.getLogger(Application.class);

    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Usage: java -jar target/raft.jar <my-port> <peer-port> ...");
            System.exit(1);
        }

        int port = Integer.parseInt(args[0]);
        Peer me = new Peer("server-" + port, new InetSocketAddress("localhost", port));

        List<Peer> peers = new ArrayList<>();
        for (int i = 1; i < args.length; i++) {
            int peerPort = Integer.parseInt(args[i]);
            peers.add(new Peer("server-" + peerPort, new InetSocketAddress("localhost", peerPort)));
        }

        long timeoutMillis = 2000;

        BasicAdapter adapter = new BasicAdapter(timeoutMillis, me, peers);
        adapter.start();
    }
}
