package com.miniredis.benchmark;

import com.miniredis.client.RedisConnection;
import com.miniredis.expiry.ExpiryManager;
import com.miniredis.persistence.AofManager;
import com.miniredis.protocol.Response;
import com.miniredis.server.RedisServer;
import com.miniredis.server.ServerConfig;
import com.miniredis.storage.Database;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Closed-loop benchmark: every client sends one command and waits for the reply
 * before sending the next. It measures client-observed round-trip latency.
 *
 * <p>Runs in-process against the real TCP server. AOF is disabled unless
 * {@code --aof} is supplied. Do not treat these numbers as universal: they
 * describe one machine, one JVM, and a loopback network.</p>
 */
public final class Benchmark {

    private static final double[] PERCENTILES = {50, 95, 99};

    private Benchmark() {
    }

    public static void main(String[] args) throws Exception {
        int requestsPerClient = 1000;
        int warmupPerClient = 100;
        boolean useAof = false;
        List<Integer> clientCounts = List.of(1, 10, 50, 100);

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--requests" -> requestsPerClient = positiveInt(args[++i], "--requests");
                case "--warmup" -> warmupPerClient = nonNegativeInt(args[++i], "--warmup");
                case "--clients" -> clientCounts = parseClientCounts(args[++i]);
                case "--aof" -> useAof = true;
                case "--help", "-h" -> {
                    System.out.println("""
                            Usage: Benchmark [--requests N] [--warmup N] [--clients 1,10,50,100] [--aof]
                              --requests  Requests per client (default 1000)
                              --warmup    Warm-up requests per client (default 100)
                              --clients   Comma-separated client counts
                              --aof       Enable AOF with fsync=everysec for comparison
                            """);
                    return;
                }
                default -> throw new IllegalArgumentException("unknown option: " + args[i]);
            }
        }

        Database database = new Database();
        ExpiryManager expiry = new ExpiryManager(database);
        expiry.start();
        AofManager aof = null;
        if (useAof) {
            Path path = Path.of("target", "benchmark-aof", "appendonly.aof");
            Files.createDirectories(path.getParent());
            Files.deleteIfExists(path);
            aof = new AofManager(path, AofManager.FsyncPolicy.EVERYSEC);
            aof.load(database);
            database.setMutationLog(aof);
        }

        ServerConfig config = new ServerConfig("127.0.0.1", 0, 128, 512, 30_000, 512);
        RedisServer server = new RedisServer(database, config);
        server.start();
        int port = server.port();

        System.out.println("Mini Redis benchmark");
        System.out.println("Mode: " + (useAof ? "AOF everysec" : "in-memory (AOF disabled)")
                + ", operation mix: 50% SET / 50% GET, loopback TCP");
        System.out.println();
        printHeader();

        try {
            for (int clients : clientCounts) {
                ScenarioResult result = runScenario(port, clients, requestsPerClient, warmupPerClient);
                System.out.println(result.toTableRow());
            }
        } finally {
            server.shutdown();
            expiry.close();
            if (aof != null) {
                aof.close();
            }
        }
    }

    private static ScenarioResult runScenario(
            int port, int clientCount, int requestsPerClient, int warmupPerClient)
            throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(clientCount);
        List<RedisConnection> connections = new ArrayList<>(clientCount);
        try {
            for (int i = 0; i < clientCount; i++) {
                connections.add(RedisConnection.connect("127.0.0.1", port, 5000));
            }

            // Warm up JIT and connection paths before measuring.
            for (int i = 0; i < clientCount; i++) {
                RedisConnection connection = connections.get(i);
                for (int j = 0; j < warmupPerClient; j++) {
                    connection.send("SET", "bench:warm:" + i + ":" + (j % 32), "warmup");
                    connection.send("GET", "bench:warm:" + i + ":" + (j % 32));
                }
            }

            long[][] latencies = new long[clientCount][requestsPerClient];
            AtomicLong errors = new AtomicLong();
            AtomicReference<Throwable> failure = new AtomicReference<>();
            CountDownLatch ready = new CountDownLatch(clientCount);
            CountDownLatch start = new CountDownLatch(1);
            CountDownLatch done = new CountDownLatch(clientCount);

            for (int c = 0; c < clientCount; c++) {
                final int clientId = c;
                final RedisConnection connection = connections.get(c);
                pool.submit(() -> {
                    try {
                        ready.countDown();
                        start.await();
                        for (int i = 0; i < requestsPerClient; i++) {
                            long began = System.nanoTime();
                            Response response;
                            if ((i & 1) == 0) {
                                response = connection.send(
                                        "SET", "bench:" + clientId + ":" + (i % 64), "value-" + i);
                            } else {
                                response = connection.send(
                                        "GET", "bench:" + clientId + ":" + ((i - 1) % 64));
                            }
                            latencies[clientId][i] = System.nanoTime() - began;
                            if (response == null || response instanceof Response.ErrorReply) {
                                errors.incrementAndGet();
                            }
                        }
                    } catch (Throwable ex) {
                        failure.compareAndSet(null, ex);
                    } finally {
                        done.countDown();
                    }
                });
            }

            ready.await(10, TimeUnit.SECONDS);
            long began = System.nanoTime();
            start.countDown();
            if (!done.await(5, TimeUnit.MINUTES)) {
                throw new IllegalStateException("benchmark timed out");
            }
            long elapsedNanos = System.nanoTime() - began;
            pool.shutdownNow();

            if (failure.get() != null) {
                throw new IllegalStateException("benchmark client failed", failure.get());
            }

            long[] merged = new long[clientCount * requestsPerClient];
            int index = 0;
            for (long[] row : latencies) {
                System.arraycopy(row, 0, merged, index, row.length);
                index += row.length;
            }
            Arrays.sort(merged);

            double seconds = elapsedNanos / 1_000_000_000.0;
            double requestsPerSecond = merged.length / seconds;
            double averageMs = Arrays.stream(merged).average().orElse(0) / 1_000_000.0;
            double[] percentileMs = new double[PERCENTILES.length];
            for (int i = 0; i < PERCENTILES.length; i++) {
                percentileMs[i] = percentile(merged, PERCENTILES[i]) / 1_000_000.0;
            }
            double errorRate = 100.0 * errors.get() / merged.length;
            return new ScenarioResult(clientCount, merged.length, requestsPerSecond,
                    averageMs, percentileMs, errorRate);
        } finally {
            for (RedisConnection connection : connections) {
                connection.close();
            }
            pool.shutdownNow();
        }
    }

    private static long percentile(long[] sorted, double percentile) {
        int index = (int) Math.ceil(percentile / 100.0 * sorted.length) - 1;
        index = Math.max(0, Math.min(sorted.length - 1, index));
        return sorted[index];
    }

    private static List<Integer> parseClientCounts(String value) {
        List<Integer> counts = new ArrayList<>();
        for (String part : value.split(",")) {
            counts.add(positiveInt(part.strip(), "--clients"));
        }
        return counts;
    }

    private static int positiveInt(String value, String option) {
        int parsed = nonNegativeInt(value, option);
        if (parsed == 0) {
            throw new IllegalArgumentException(option + " must be positive");
        }
        return parsed;
    }

    private static int nonNegativeInt(String value, String option) {
        try {
            int parsed = Integer.parseInt(value);
            if (parsed < 0) {
                throw new IllegalArgumentException(option + " must not be negative");
            }
            return parsed;
        } catch (NumberFormatException ex) {
            throw new IllegalArgumentException(option + " must be an integer");
        }
    }

    private record ScenarioResult(
            int clients,
            int requests,
            double requestsPerSecond,
            double averageMs,
            double[] percentileMs,
            double errorRate) {

        String toTableRow() {
            return String.format(Locale.ROOT,
                    "| %7d | %8d | %12.0f | %7.3f | %6.3f | %6.3f | %6.3f | %6.2f%% |",
                    clients, requests, requestsPerSecond, averageMs,
                    percentileMs[0], percentileMs[1], percentileMs[2], errorRate);
        }
    }

    private static void printHeader() {
        System.out.println("| Clients | Requests | Requests/sec | Avg ms | P50 ms | P95 ms | P99 ms | Errors |");
    }
}
