package org.gautelis.raft;

import org.gautelis.raft.model.LogEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Properties;

public final class FileLogStore implements LogStore {
    private static final Logger log = LoggerFactory.getLogger(FileLogStore.class);

    private final Path logFile;
    private final Path metaFile;
    private final List<LogEntry> entries = new ArrayList<>();
    private long snapshotIndex = 0L;
    private long snapshotTerm = 0L;
    private byte[] snapshotData = new byte[0];

    public FileLogStore(Path logFile) {
        // Two-file layout:
        // - log file for post-snapshot entries
        // - meta file for snapshot index/term/data
        // This preserves Raft global index semantics while allowing compaction.
        this.logFile = logFile;
        this.metaFile = logFile.resolveSibling(logFile.getFileName() + ".meta");
        load();
    }

    @Override
    public synchronized long snapshotIndex() {
        return snapshotIndex;
    }

    @Override
    public synchronized long snapshotTerm() {
        return snapshotTerm;
    }

    @Override
    public synchronized long lastIndex() {
        return snapshotIndex + entries.size();
    }

    @Override
    public synchronized long lastTerm() {
        if (entries.isEmpty()) return snapshotTerm;
        return entries.getLast().getTerm();
    }

    @Override
    public synchronized long termAt(long index) {
        // Index 0 is the Raft sentinel used by empty-prefix AppendEntries checks.
        if (index == 0) return 0L;
        if (index == snapshotIndex) {
            return snapshotTerm;
        }
        if (index < snapshotIndex) {
            throw new IndexOutOfBoundsException("No entry at compacted index " + index + " (snapshotIndex=" + snapshotIndex + ")");
        }
        int i = Math.toIntExact(index - snapshotIndex - 1);
        if (i < 0 || i >= entries.size()) {
            throw new IndexOutOfBoundsException("No entry at index " + index + " (snapshotIndex=" + snapshotIndex + ", size=" + entries.size() + ")");
        }
        return entries.get(i).getTerm();
    }

    @Override
    public synchronized LogEntry entryAt(long index) {
        if (index == 0 || index <= snapshotIndex) {
            throw new IndexOutOfBoundsException("No entry at index 0");
        }
        int i = Math.toIntExact(index - snapshotIndex - 1);
        if (i < 0 || i >= entries.size()) {
            throw new IndexOutOfBoundsException("No entry at index " + index + " (snapshotIndex=" + snapshotIndex + ", size=" + entries.size() + ")");
        }
        return entries.get(i);
    }

    @Override
    public synchronized void append(List<LogEntry> entries) {
        this.entries.addAll(entries);
        persist();
    }

    @Override
    public synchronized void truncateFrom(long index) {
        // Figure 2 AppendEntries receiver step 3:
        // delete conflicting entry and all that follow it.
        if (index <= 0) {
            entries.clear();
            persist();
            return;
        }
        if (index <= snapshotIndex + 1) {
            entries.clear();
            persist();
            return;
        }
        int from = Math.toIntExact(index - snapshotIndex - 1);
        if (from >= entries.size()) {
            return;
        }
        entries.subList(from, entries.size()).clear();
        persist();
    }

    @Override
    public synchronized List<LogEntry> entriesFrom(long index) {
        if (index <= snapshotIndex) index = snapshotIndex + 1;
        int from = Math.toIntExact(index - snapshotIndex - 1);
        if (from >= entries.size()) {
            return List.of();
        }
        return new ArrayList<>(entries.subList(from, entries.size()));
    }

    @Override
    public synchronized void compactUpTo(long index) {
        // Move snapshot boundary forward while keeping term lookup at snapshotIndex.
        if (index <= snapshotIndex) {
            return;
        }
        long target = Math.min(index, lastIndex());
        long targetTerm = termAt(target);

        if (target >= lastIndex()) {
            entries.clear();
            snapshotIndex = target;
            snapshotTerm = targetTerm;
            persist();
            return;
        }

        int removeCount = Math.toIntExact(target - snapshotIndex);
        entries.subList(0, removeCount).clear();
        snapshotIndex = target;
        snapshotTerm = targetTerm;
        snapshotData = new byte[0];
        persist();
    }

    @Override
    public synchronized byte[] snapshotData() {
        return Arrays.copyOf(snapshotData, snapshotData.length);
    }

    @Override
    public synchronized void installSnapshot(long lastIncludedIndex, long lastIncludedTerm, byte[] snapshotData) {
        // Keep suffix only if local history agrees at snapshot boundary.
        // Otherwise local suffix is inconsistent and must be dropped.
        if (lastIncludedIndex < snapshotIndex) {
            return;
        }

        long previousSnapshotIndex = snapshotIndex;
        List<LogEntry> suffix = List.of();
        if (lastIncludedIndex < lastIndex()) {
            if (lastIncludedIndex >= previousSnapshotIndex && termAt(lastIncludedIndex) == lastIncludedTerm) {
                suffix = entriesFrom(lastIncludedIndex + 1);
            }
        }

        entries.clear();
        entries.addAll(suffix);
        snapshotIndex = lastIncludedIndex;
        snapshotTerm = lastIncludedTerm;
        this.snapshotData = snapshotData == null ? new byte[0] : Arrays.copyOf(snapshotData, snapshotData.length);
        persist();
    }

    private void load() {
        // Replay persisted protobuf entries in-order to rebuild in-memory suffix.
        loadMeta();
        if (!Files.exists(logFile)) {
            return;
        }

        try (DataInputStream in = new DataInputStream(Files.newInputStream(logFile))) {
            while (true) {
                int length = in.readInt();
                byte[] bytes = new byte[length];
                in.readFully(bytes);
                var proto = org.gautelis.raft.proto.LogEntry.parseFrom(bytes);
                entries.add(ProtoMapper.fromProto(proto));
            }
        } catch (EOFException ignored) {
            // done
        } catch (IOException e) {
            throw new IllegalStateException("Failed loading log from " + logFile, e);
        }
    }

    private void loadMeta() {
        // Snapshot metadata is persisted separately from log suffix bytes.
        if (!Files.exists(metaFile)) {
            snapshotIndex = 0L;
            snapshotTerm = 0L;
            return;
        }
        Properties props = new Properties();
        try (var in = Files.newInputStream(metaFile)) {
            props.load(in);
            snapshotIndex = Long.parseLong(props.getProperty("snapshotIndex", "0"));
            snapshotTerm = Long.parseLong(props.getProperty("snapshotTerm", "0"));
            snapshotData = Base64.getDecoder().decode(props.getProperty("snapshotData", ""));
        } catch (IOException | NumberFormatException e) {
            throw new IllegalStateException("Failed loading log metadata from " + metaFile, e);
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException("Failed decoding snapshot data from " + metaFile, e);
        }
    }

    private void persistMeta() throws IOException {
        Path parent = metaFile.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        Properties props = new Properties();
        props.setProperty("snapshotIndex", Long.toString(snapshotIndex));
        props.setProperty("snapshotTerm", Long.toString(snapshotTerm));
        props.setProperty("snapshotData", Base64.getEncoder().encodeToString(snapshotData));

        Path tmp = metaFile.resolveSibling(metaFile.getFileName() + ".tmp");
        try (var out = Files.newOutputStream(tmp)) {
            props.store(out, "raft log metadata");
        }
        Files.move(tmp, metaFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    }

    private void persist() {
        try {
            Path parent = logFile.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            persistMeta();

            Path tmp = logFile.resolveSibling(logFile.getFileName() + ".tmp");
            try (DataOutputStream out = new DataOutputStream(Files.newOutputStream(tmp))) {
                for (LogEntry entry : entries) {
                    // Length-prefix framing keeps reload simple and robust.
                    byte[] bytes = ProtoMapper.toProto(entry).toByteArray();
                    out.writeInt(bytes.length);
                    out.write(bytes);
                }
            }
            Files.move(tmp, logFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            log.error("Failed persisting log to {}", logFile, e);
            throw new IllegalStateException("Failed persisting log to " + logFile, e);
        }
    }
}
