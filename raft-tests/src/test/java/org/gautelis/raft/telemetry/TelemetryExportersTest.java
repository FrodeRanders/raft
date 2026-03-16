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
package org.gautelis.raft.telemetry;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.gautelis.raft.ClusterConfiguration;
import org.gautelis.raft.RaftNode;
import org.gautelis.raft.protocol.Peer;
import org.gautelis.raft.protocol.TelemetryPeerStats;
import org.gautelis.raft.protocol.TelemetryReplicationStatus;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertTrue;

class TelemetryExportersTest {

    @Test
    void prometheusExporterServesLatestMetricsSnapshot() throws Exception {
        System.out.println("TC: Prometheus exporter: scrape endpoint exposes latest Raft metrics");
        PrometheusTelemetryExporter exporter = new PrometheusTelemetryExporter();
        try {
            ClusterConfiguration active = ClusterConfiguration.stable(List.of(
                    new Peer("A", new InetSocketAddress("127.0.0.1", 10080), Peer.Role.VOTER),
                    new Peer("B", new InetSocketAddress("127.0.0.1", 10081), Peer.Role.LEARNER),
                    new Peer("C", new InetSocketAddress("127.0.0.1", 10082), Peer.Role.VOTER)
            ));
            ClusterConfiguration target = active.transitionTo(List.of(
                    new Peer("A", new InetSocketAddress("127.0.0.1", 10080), Peer.Role.VOTER),
                    new Peer("B", new InetSocketAddress("127.0.0.1", 10081), Peer.Role.VOTER),
                    new Peer("C", new InetSocketAddress("127.0.0.1", 10082), Peer.Role.LEARNER)
            ));
            RaftNode.TelemetrySnapshot snapshot = new RaftNode.TelemetrySnapshot(
                    1000L,
                    7L,
                    "A",
                    "LEADER",
                    "A",
                    "",
                    false,
                    false,
                    12L,
                    12L,
                    13L,
                    7L,
                    0L,
                    0L,
                    900L,
                    1200L,
                    active,
                    target,
                    500L,
                    List.of(),
                    List.of(),
                    List.of(new TelemetryReplicationStatus("B", 14L, 12L, true, 995L, 0, 0L))
            );
            exporter.publish(snapshot, List.of(new TelemetryPeerStats("B", "AppendEntriesRequest", 5L, 1.5, 1.0, 2.0, 10.0)));

            HttpClient client = HttpClient.newHttpClient();
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create("http://127.0.0.1:" + exporter.getListenPort() + "/metrics"))
                    .GET()
                    .build();
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

            assertTrue(response.body().contains("raft_term{peer_id=\"A\"} 7"));
            assertTrue(response.body().contains("raft_replication_match_index{peer_id=\"A\",remote_peer_id=\"B\"} 12"));
            assertTrue(response.body().contains("raft_transport_response_mean_millis{peer_id=\"A\",remote_peer_id=\"B\",rpc_type=\"AppendEntriesRequest\"} 1.500"));
            assertTrue(response.body().contains("raft_members_promoting{peer_id=\"A\"} 1"));
            assertTrue(response.body().contains("raft_members_demoting{peer_id=\"A\"} 1"));
            assertTrue(response.body().contains("raft_reconfiguration_age_millis{peer_id=\"A\"} 500"));
        } finally {
            exporter.close();
        }
    }

    @Test
    void otlpExporterPostsMetricsPayloadToCollectorEndpoint() throws Exception {
        System.out.println("TC: OTLP exporter: configured mode posts OpenTelemetry metrics to collector endpoint");
        AtomicReference<String> body = new AtomicReference<>("");
        AtomicReference<String> header = new AtomicReference<>("");
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/v1/metrics", exchange -> handleCollectorRequest(exchange, body, header));
        server.start();

        String previousExporter = System.getProperty("raft.telemetry.exporter");
        String previousEndpoint = System.getProperty("raft.telemetry.otlp.endpoint");
        String previousHeaders = System.getProperty("raft.telemetry.otlp.headers");
        System.setProperty("raft.telemetry.exporter", "otlp");
        System.setProperty("raft.telemetry.otlp.endpoint", "http://127.0.0.1:" + server.getAddress().getPort());
        System.setProperty("raft.telemetry.otlp.headers", "Authorization=Bearer demo-token");
        try {
            TelemetryExporter exporter = TelemetryExporters.createFromProperties();
            assertTrue(exporter.isEnabled());
            exporter.publish(sampleSnapshot(), List.of(new TelemetryPeerStats("B", "AppendEntriesRequest", 5L, 1.5, 1.0, 2.0, 10.0)));
            exporter.close();

            assertTrue(header.get().contains("Bearer demo-token"));
            assertTrue(body.get().contains("\"resourceMetrics\""));
            assertTrue(body.get().contains("\"key\":\"service.name\""));
            assertTrue(body.get().contains("\"stringValue\":\"raft\""));
            assertTrue(body.get().contains("\"name\":\"raft.term\""));
            assertTrue(body.get().contains("\"name\":\"raft.members.promoting\""));
            assertTrue(body.get().contains("\"name\":\"raft.members.demoting\""));
            assertTrue(body.get().contains("\"name\":\"raft.reconfiguration.age.millis\""));
            assertTrue(body.get().contains("\"key\":\"remote.peer.id\""));
            assertTrue(body.get().contains("\"stringValue\":\"B\""));
            assertTrue(body.get().contains("\"key\":\"rpc.type\""));
            assertTrue(body.get().contains("\"stringValue\":\"AppendEntriesRequest\""));
        } finally {
            server.stop(0);
            restoreProperty("raft.telemetry.exporter", previousExporter);
            restoreProperty("raft.telemetry.otlp.endpoint", previousEndpoint);
            restoreProperty("raft.telemetry.otlp.headers", previousHeaders);
        }
    }

    @Test
    void otelLogExporterRemainsAvailableAsExplicitFallback() {
        System.out.println("TC: OpenTelemetry log exporter: explicit fallback mode remains available without collector infrastructure");
        String previous = System.getProperty("raft.telemetry.exporter");
        System.setProperty("raft.telemetry.exporter", "otel-log");
        try {
            TelemetryExporter exporter = TelemetryExporters.createFromProperties();
            assertTrue(exporter.isEnabled());
            exporter.publish(sampleSnapshot(), List.of());
            exporter.close();
        } finally {
            restoreProperty("raft.telemetry.exporter", previous);
        }
    }

    private static RaftNode.TelemetrySnapshot sampleSnapshot() {
        ClusterConfiguration active = ClusterConfiguration.stable(List.of(
                new Peer("A", null, Peer.Role.VOTER),
                new Peer("B", null, Peer.Role.LEARNER),
                new Peer("C", null, Peer.Role.VOTER)
        ));
        ClusterConfiguration target = active.transitionTo(List.of(
                new Peer("A", null, Peer.Role.VOTER),
                new Peer("B", null, Peer.Role.VOTER),
                new Peer("C", null, Peer.Role.LEARNER)
        ));
        return new RaftNode.TelemetrySnapshot(
                1000L,
                7L,
                "A",
                "LEADER",
                "A",
                "",
                false,
                false,
                12L,
                12L,
                13L,
                7L,
                0L,
                0L,
                900L,
                1200L,
                active,
                target,
                500L,
                List.of(),
                List.of(),
                List.of(new TelemetryReplicationStatus("B", 14L, 12L, true, 995L, 0, 0L))
        );
    }

    private static void handleCollectorRequest(HttpExchange exchange, AtomicReference<String> body, AtomicReference<String> header) throws IOException {
        body.set(new String(exchange.getRequestBody().readAllBytes()));
        header.set(exchange.getRequestHeaders().getFirst("Authorization"));
        exchange.sendResponseHeaders(200, -1);
        exchange.close();
    }

    private static void restoreProperty(String key, String value) {
        if (value == null) {
            System.clearProperty(key);
        } else {
            System.setProperty(key, value);
        }
    }
}
