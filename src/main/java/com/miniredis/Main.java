package com.miniredis;

import com.miniredis.expiry.ExpiryManager;
import com.miniredis.persistence.AofManager;
import com.miniredis.server.RedisServer;
import com.miniredis.server.ServerConfig;
import com.miniredis.storage.Database;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;

/** Server entry point and command-line option parsing. */
public final class Main {

    private static final String USAGE = """
            Usage: java -jar mini-redis-1.0.0.jar [options]

            Options:
              --port PORT          TCP port (default: 6379)
              --bind ADDRESS       Bind address (default: 127.0.0.1)
              --aof PATH           Append-only file (default: data/appendonly.aof)
              --no-aof             Disable persistence
              --fsync POLICY       always | everysec | no (default: always)
              --workers N          Maximum worker threads (default: 128)
              --queue N            Pending connection queue size (default: 256)
              --max-clients N      Maximum live connections (default: 256)
              --help               Show this help
            """;

    private Main() {
    }

    public static void main(String[] args) {
        try {
            run(args);
        } catch (Exception ex) {
            System.err.println("Fatal: " + ex.getMessage());
            System.exit(1);
        }
    }

    private static void run(String[] args) throws Exception {
        Config config = parse(args);
        if (config == null) {
            return;
        }

        Database database = new Database();
        final AofManager aof = openAof(database, config);
        final ExpiryManager expiry = new ExpiryManager(database);
        expiry.start();

        ServerConfig serverConfig = new ServerConfig(
                config.bind,
                config.port,
                config.workers,
                config.queueCapacity,
                config.socketTimeoutMillis,
                config.maxClients);
        final RedisServer server = new RedisServer(database, serverConfig);
        server.start();

        System.out.println("Mini Redis 1.0.0");
        System.out.println("Listening on " + config.bind + ":" + server.port());
        if (aof == null) {
            System.out.println("Persistence: disabled");
        } else {
            System.out.println("Persistence: " + aof.path() + " (fsync=" + config.fsync.name().toLowerCase() + ")");
        }
        System.out.println("CLI: java -cp target/mini-redis-1.0.0.jar com.miniredis.client.MiniRedisClient --port " + server.port());
        System.out.println("Press Ctrl+C to stop.");

        AtomicBoolean cleaned = new AtomicBoolean();
        Runnable cleanup = () -> {
            if (cleaned.compareAndSet(false, true)) {
                server.shutdown();
                expiry.close();
                if (aof != null) {
                    aof.close();
                }
            }
        };
        Runtime.getRuntime().addShutdownHook(new Thread(cleanup, "mini-redis-shutdown"));

        server.awaitTermination();
        cleanup.run();
    }

    private static AofManager openAof(Database database, Config config) throws IOException {
        if (!config.aofEnabled) {
            return null;
        }
        AofManager aof = new AofManager(Path.of(config.aofPath), config.fsync);
        AofManager.ReplayStats stats = aof.load(database);
        database.setMutationLog(aof);
        System.out.println("AOF replay: " + stats.recordsApplied() + " records applied, "
                + stats.recordsSkipped() + " skipped, "
                + stats.bytesTruncated() + " bytes truncated");
        return aof;
    }

    private static Config parse(String[] args) {
        Config config = new Config();
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--port" -> config.port = parsePort(value(args, ++i, "--port"));
                case "--bind" -> config.bind = value(args, ++i, "--bind");
                case "--aof" -> {
                    config.aofPath = value(args, ++i, "--aof");
                    config.aofEnabled = true;
                }
                case "--no-aof" -> config.aofEnabled = false;
                case "--fsync" -> config.fsync = AofManager.FsyncPolicy.parse(value(args, ++i, "--fsync"));
                case "--workers" -> config.workers = parsePositive(value(args, ++i, "--workers"), "--workers");
                case "--queue" -> config.queueCapacity = parseNonNegative(value(args, ++i, "--queue"), "--queue");
                case "--max-clients" -> config.maxClients = parsePositive(value(args, ++i, "--max-clients"), "--max-clients");
                case "--help", "-h" -> {
                    System.out.print(USAGE);
                    return null;
                }
                default -> throw new IllegalArgumentException("unknown option: " + args[i]);
            }
        }
        if (config.bind.isBlank()) {
            throw new IllegalArgumentException("--bind must not be blank");
        }
        return config;
    }

    private static String value(String[] args, int index, String option) {
        if (index >= args.length) {
            throw new IllegalArgumentException(option + " requires a value");
        }
        return args[index];
    }

    private static int parsePort(String value) {
        try {
            int port = Integer.parseInt(value);
            if (port < 0 || port > 65535) {
                throw new IllegalArgumentException("--port must be between 0 and 65535");
            }
            return port;
        } catch (NumberFormatException ex) {
            throw new IllegalArgumentException("--port must be an integer");
        }
    }

    private static int parsePositive(String value, String option) {
        try {
            int parsed = Integer.parseInt(value);
            if (parsed < 1) {
                throw new IllegalArgumentException(option + " must be positive");
            }
            return parsed;
        } catch (NumberFormatException ex) {
            throw new IllegalArgumentException(option + " must be an integer");
        }
    }

    private static int parseNonNegative(String value, String option) {
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

    private static final class Config {
        String bind = "127.0.0.1";
        int port = 6379;
        String aofPath = "data/appendonly.aof";
        boolean aofEnabled = true;
        AofManager.FsyncPolicy fsync = AofManager.FsyncPolicy.ALWAYS;
        int workers = 128;
        int queueCapacity = 256;
        int maxClients = 256;
        int socketTimeoutMillis = 300_000;
    }
}
