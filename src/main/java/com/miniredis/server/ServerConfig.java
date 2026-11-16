package com.miniredis.server;

/** Immutable networking configuration for {@link RedisServer}. */
public record ServerConfig(
        String bindAddress,
        int port,
        int workerThreads,
        int queueCapacity,
        int socketTimeoutMillis,
        int maxConnections) {

    public ServerConfig {
        if (bindAddress == null || bindAddress.isBlank()) {
            throw new IllegalArgumentException("bind address must not be blank");
        }
        if (port < 0 || port > 65535) {
            throw new IllegalArgumentException("port must be between 0 and 65535");
        }
        if (workerThreads < 1) {
            throw new IllegalArgumentException("workerThreads must be positive");
        }
        if (queueCapacity < 0) {
            throw new IllegalArgumentException("queueCapacity must not be negative");
        }
        if (socketTimeoutMillis < 0) {
            throw new IllegalArgumentException("socketTimeoutMillis must not be negative");
        }
        if (maxConnections < 1) {
            throw new IllegalArgumentException("maxConnections must be positive");
        }
    }

    public static ServerConfig defaults() {
        return new ServerConfig("127.0.0.1", 6379, 128, 256, 300_000, 256);
    }

    public static ServerConfig port(int port) {
        return new ServerConfig("127.0.0.1", port, 128, 256, 300_000, 256);
    }
}
