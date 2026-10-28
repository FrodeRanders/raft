package org.gautelis.raft.storage;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;

final class DurableFiles {
    interface Writer { void write(DataOutputStream out) throws IOException; }

    static void replace(Path path, Writer writer) throws IOException {
        Path absolute = path.toAbsolutePath();
        Files.createDirectories(absolute.getParent());
        Path temporary = absolute.resolveSibling(absolute.getFileName() + ".tmp");
        try (FileChannel channel = FileChannel.open(temporary, StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)) {
            DataOutputStream out = new DataOutputStream(Channels.newOutputStream(channel));
            writer.write(out);
            out.flush();
            channel.force(true);
        }
        Files.move(temporary, absolute, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        try (FileChannel directory = FileChannel.open(absolute.getParent(), StandardOpenOption.READ)) {
            directory.force(true);
        }
    }
}
