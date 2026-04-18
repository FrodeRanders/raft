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
package org.gautelis.raft;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JepsenMavenIntegrationTest {
    private static final Pattern RESULTS_PATTERN = Pattern.compile("Wrote (.+/results\\.edn)");

    @Test
    @Timeout(value = 3, unit = TimeUnit.MINUTES)
    void smokeRunPassesUnderSurefire() throws Exception {
        runJepsen("smoke", false, 17080, List.of());
    }

    @Test
    @Timeout(value = 3, unit = TimeUnit.MINUTES)
    void crashRestartRunPassesUnderSurefire() throws Exception {
        runJepsen("crash", false, 17180, List.of("--nemesis", "crash-restart", "--nemesis-interval", "3"));
    }

    @Test
    @Timeout(value = 3, unit = TimeUnit.MINUTES)
    void partitionRunPassesUnderSurefire() throws Exception {
        runJepsen("partition", true, 17280, List.of("--nemesis", "partition-one", "--nemesis-interval", "3"));
    }

    private static void runJepsen(String name, boolean requiresSudo, int basePort, List<String> extraArgs) throws Exception {
        Assumptions.assumeFalse(Boolean.getBoolean("skipJepsenTests"),
                "Jepsen tests disabled via -DskipJepsenTests=true");

        Path repoRoot = repoRoot();
        Path jepsenDir = repoRoot.resolve("jepsen");
        Path jar = repoRoot.resolve("raft-dist").resolve("target").resolve("raft-1.0-SNAPSHOT.jar");
        assertTrue(Files.isRegularFile(jar),
                "Expected runnable raft-dist jar at " + jar + ". Maven should build it before Surefire runs.");

        verifyClojureCli(jepsenDir);
        if (requiresSudo) {
            verifyPartitionPrivilege(repoRoot);
        }

        Path workdir = Path.of("/tmp", "raft-jepsen-mvn-" + name);
        List<String> command = new ArrayList<>();
        command.add(jepsenDir.resolve("run-local.sh").toString());
        command.add("--time-limit");
        command.add("10");
        command.add("--concurrency");
        command.add("10");
        command.add("--node-count");
        command.add("5");
        command.add("--base-port");
        command.add(Integer.toString(basePort));
        command.add("--workdir");
        command.add(workdir.toString());
        command.addAll(extraArgs);

        ExecResult run = execJepsen(jepsenDir, command);
        assertEquals(0, run.exitCode(),
                "Jepsen run failed.\nCommand: " + String.join(" ", command) + "\nOutput:\n" + run.output());

        Path results = extractResultsPath(run.output());
        String contents = Files.readString(results, StandardCharsets.UTF_8);
        assertTrue(contents.contains(":linearizable {:valid? true"),
                "Jepsen linearizability check failed.\nResults:\n" + contents + "\nOutput:\n" + run.output());
        assertTrue(contents.contains(":timeline {:valid? true}"),
                "Jepsen timeline check failed.\nResults:\n" + contents + "\nOutput:\n" + run.output());
        assertTrue(contents.contains(":valid? true"),
                "Jepsen run was not valid.\nResults:\n" + contents + "\nOutput:\n" + run.output());
    }

    private static void verifyClojureCli(Path jepsenDir) throws Exception {
        ExecResult result = exec(jepsenDir, List.of("clojure", "-Sdescribe"));
        assertEquals(0, result.exitCode(),
                "Clojure CLI is required for Maven-driven Jepsen tests.\n" +
                        "Install it before running mvn test.\nOutput:\n" + result.output());
    }

    private static void verifyPartitionPrivilege(Path repoRoot) throws Exception {
        Path helper = repoRoot.resolve("jepsen").resolve("scripts").resolve("partition.sh");
        ExecResult result = exec(repoRoot, List.of("sudo", "-n", helper.toString(), "heal"));
        assertEquals(0, result.exitCode(),
                "Partition Jepsen tests require non-interactive sudo for " + helper + ".\n" +
                        "Configure sudoers as documented in docs/jepsen-workflow.md and jepsen/README.md.\nOutput:\n" + result.output());
    }

    private static Path extractResultsPath(String output) {
        Path found = maybeExtractResultsPath(output);
        assertTrue(found != null && Files.isRegularFile(found),
                "Could not locate Jepsen results.edn in output:\n" + output);
        return found;
    }

    private static Path maybeExtractResultsPath(String output) {
        Matcher matcher = RESULTS_PATTERN.matcher(output);
        Path found = null;
        while (matcher.find()) {
            found = Path.of(matcher.group(1));
        }
        return found;
    }

    private static Path repoRoot() {
        String configured = System.getProperty("raft.repoRoot");
        assertTrue(configured != null && !configured.isBlank(),
                "Missing raft.repoRoot system property");
        return Path.of(configured).toAbsolutePath().normalize();
    }

    private static ExecResult exec(Path directory, List<String> command) throws IOException, InterruptedException {
        Process process = new ProcessBuilder(command)
                .directory(directory.toFile())
                .redirectErrorStream(true)
                .start();

        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        try (InputStream input = process.getInputStream()) {
            input.transferTo(buffer);
        }
        boolean finished = process.waitFor(3, TimeUnit.MINUTES);
        if (!finished) {
            process.destroyForcibly();
            throw new IllegalStateException("Timed out waiting for command: " + String.join(" ", command));
        }
        return new ExecResult(process.exitValue(), buffer.toString(StandardCharsets.UTF_8));
    }

    private static ExecResult execJepsen(Path directory, List<String> command) throws IOException, InterruptedException {
        Process process = new ProcessBuilder(command)
                .directory(directory.toFile())
                .redirectErrorStream(true)
                .start();

        StringBuffer output = new StringBuffer();
        Thread reader = new Thread(() -> {
            try (InputStream input = process.getInputStream()) {
                byte[] chunk = new byte[8192];
                int read;
                while ((read = input.read(chunk)) >= 0) {
                    synchronized (output) {
                        output.append(new String(chunk, 0, read, StandardCharsets.UTF_8));
                    }
                }
            } catch (IOException ignored) {
            }
        }, "jepsen-output-reader");
        reader.setDaemon(true);
        reader.start();

        long deadline = System.nanoTime() + TimeUnit.MINUTES.toNanos(5);
        while (System.nanoTime() < deadline) {
            if (process.waitFor(1, TimeUnit.SECONDS)) {
                reader.join(5000);
                return new ExecResult(process.exitValue(), synchronizedOutput(output));
            }

            Path results = maybeExtractResultsPath(synchronizedOutput(output));
            if (results != null && Files.isRegularFile(results)) {
                if (!process.waitFor(5, TimeUnit.SECONDS)) {
                    process.destroy();
                    if (!process.waitFor(5, TimeUnit.SECONDS)) {
                        process.destroyForcibly();
                        process.waitFor(5, TimeUnit.SECONDS);
                    }
                }
                reader.join(5000);
                return new ExecResult(0, synchronizedOutput(output));
            }
        }

        process.destroyForcibly();
        process.waitFor(5, TimeUnit.SECONDS);
        reader.join(5000);
        throw new IllegalStateException("Timed out waiting for command: " + String.join(" ", command)
                + "\nOutput so far:\n" + synchronizedOutput(output));
    }

    private static String synchronizedOutput(StringBuffer output) {
        synchronized (output) {
            return output.toString();
        }
    }

    private record ExecResult(int exitCode, String output) {
    }
}
