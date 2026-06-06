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
package org.gautelis.raft.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Optional;
import java.util.Properties;

/**
 * Persists the current term and voted-for state to disk across restarts.
 *
 * <p>This store is small but safety-critical. Raft election safety depends on
 * preserving term and vote before a restarted node can participate in another
 * election.</p>
 */
public final class FilePersistentStateStore implements PersistentStateStore {
    private static final Logger log = LoggerFactory.getLogger(FilePersistentStateStore.class);

    private static final String TERM = "currentTerm";
    private static final String VOTED_FOR = "votedFor";

    private final Path stateFile;
    private long currentTerm;
    private String votedFor;

    public FilePersistentStateStore(Path stateFile) {
        this.stateFile = stateFile;
        load();
    }

    @Override
    public synchronized long currentTerm() {
        return currentTerm;
    }

    @Override
    public synchronized void setCurrentTerm(long term) {
        // Persist immediately: a higher term observed from any RPC must survive
        // restart before the node can grant or solicit votes again.
        this.currentTerm = term;
        persist();
    }

    @Override
    public synchronized Optional<String> votedFor() {
        return Optional.ofNullable(votedFor);
    }

    @Override
    public synchronized void setVotedFor(String peerId) {
        // Persist immediately to prevent double voting in the same term after a
        // crash. Null means no vote is recorded for the current term.
        this.votedFor = peerId;
        persist();
    }

    private void load() {
        if (!Files.exists(stateFile)) {
            return;
        }
        Properties props = new Properties();
        try (InputStream in = Files.newInputStream(stateFile)) {
            props.load(in);
            String termRaw = props.getProperty(TERM, "0");
            currentTerm = Long.parseLong(termRaw);
            votedFor = props.getProperty(VOTED_FOR);
            if (votedFor != null && votedFor.isBlank()) {
                votedFor = null;
            }

        } catch (IOException | NumberFormatException e) {
            throw new IllegalStateException("Failed loading persistent state from " + stateFile, e);
        }
    }

    private void persist() {
        try {
            Path parent = stateFile.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }

            Properties props = new Properties();
            props.setProperty(TERM, Long.toString(currentTerm));
            props.setProperty(VOTED_FOR, votedFor == null ? "" : votedFor);

            Path tmp = stateFile.resolveSibling(stateFile.getFileName() + ".tmp");
            try (OutputStream out = Files.newOutputStream(tmp)) {
                props.store(out, "raft persistent state");
            }
            // Replace atomically when the platform supports it so readers never
            // observe a half-written term/vote file.
            Files.move(tmp, stateFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);

        } catch (IOException e) {
            log.error("Failed persisting state to {}", stateFile, e);
            throw new IllegalStateException("Failed persisting state to " + stateFile, e);
        }
    }
}
