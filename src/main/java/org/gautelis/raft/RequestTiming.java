package org.gautelis.raft;

final class RequestTiming {
    private final String peerId;
    private final long startNanos;

    RequestTiming(String peerId, long startNanos) {
        this.peerId = peerId;
        this.startNanos = startNanos;
    }

    String peerId() {
        return peerId;
    }

    long startNanos() {
        return startNanos;
    }
}
