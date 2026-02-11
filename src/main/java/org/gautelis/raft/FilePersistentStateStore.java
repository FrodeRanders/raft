package org.gautelis.raft;

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
        this.currentTerm = term;
        persist();
    }

    @Override
    public synchronized Optional<String> votedFor() {
        return Optional.ofNullable(votedFor);
    }

    @Override
    public synchronized void setVotedFor(String peerId) {
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
            Files.move(tmp, stateFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            log.error("Failed persisting state to {}", stateFile, e);
            throw new IllegalStateException("Failed persisting state to " + stateFile, e);
        }
    }
}
