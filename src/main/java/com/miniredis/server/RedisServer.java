package com.miniredis.server;

import com.miniredis.command.CommandRegistry;
import com.miniredis.protocol.RespProtocol;
import com.miniredis.protocol.Response;
import com.miniredis.storage.Database;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Blocking-I/O TCP server with a bounded worker pool. Every connection is
 * serviced by one pooled thread, so a slow client cannot block other clients.
 * The pool size bounds resource use; excess connections wait in a bounded queue
 * and are rejected with an error if the queue is full.
 */
public final class RedisServer implements AutoCloseable {

    private static final Logger LOG = Logger.getLogger(RedisServer.class.getName());

    private final Database database;
    private final ServerConfig config;
    private final CommandRegistry registry;
    private final ThreadPoolExecutor workers;
    private final Set<Socket> clients = ConcurrentHashMap.newKeySet();
    private final AtomicInteger activeClients = new AtomicInteger();

    private volatile boolean running;
    private volatile int actualPort;
    private ServerSocket serverSocket;
    private Thread acceptThread;

    public RedisServer(Database database, ServerConfig config) {
        this.database = database;
        this.config = config;
        this.registry = new CommandRegistry(database);

        AtomicInteger threadNumber = new AtomicInteger();
        ThreadFactory threadFactory = runnable -> {
            Thread thread = new Thread(runnable,
                    "mini-redis-worker-" + threadNumber.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        };
        this.workers = new ThreadPoolExecutor(
                config.workerThreads(),
                config.workerThreads(),
                0L,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(config.queueCapacity()),
                threadFactory,
                new ThreadPoolExecutor.AbortPolicy());
    }

    public synchronized void start() throws IOException {
        if (running) {
            throw new IllegalStateException("server is already running");
        }

        ServerSocket socket = new ServerSocket();
        socket.setReuseAddress(true);
        socket.bind(new InetSocketAddress(config.bindAddress(), config.port()), 256);
        this.serverSocket = socket;
        this.actualPort = socket.getLocalPort();
        this.running = true;

        acceptThread = new Thread(this::acceptLoop, "mini-redis-accept");
        acceptThread.setDaemon(true);
        acceptThread.start();
    }

    private void acceptLoop() {
        while (running) {
            try {
                Socket client = serverSocket.accept();
                configure(client);
                if (activeClients.incrementAndGet() > config.maxConnections()) {
                    activeClients.decrementAndGet();
                    rejectBusy(client);
                    continue;
                }
                clients.add(client);
                try {
                    workers.execute(() -> handle(client));
                } catch (RejectedExecutionException ex) {
                    activeClients.decrementAndGet();
                    clients.remove(client);
                    rejectBusy(client);
                }
            } catch (SocketException ex) {
                if (running) {
                    LOG.log(Level.WARNING, "Server socket failed", ex);
                }
                break;
            } catch (IOException ex) {
                if (running) {
                    LOG.log(Level.WARNING, "Error accepting client", ex);
                }
            }
        }
    }

    private void handle(Socket client) {
        try {
            new ClientHandler(client, registry).run();
        } finally {
            clients.remove(client);
            activeClients.decrementAndGet();
            closeQuietly(client);
        }
    }

    private void configure(Socket client) throws SocketException {
        client.setTcpNoDelay(true);
        client.setKeepAlive(true);
        client.setSoTimeout(config.socketTimeoutMillis());
    }

    private void rejectBusy(Socket client) {
        try (client) {
            OutputStream out = client.getOutputStream();
            RespProtocol.writeResponse(out, Response.error("ERR server is busy"));
            out.flush();
        } catch (IOException ignored) {
            // The client is going away anyway.
        }
    }

    public int port() {
        return actualPort;
    }

    public boolean isRunning() {
        return running;
    }

    public Database database() {
        return database;
    }

    /** Stops accepting, closes active clients, and drains the worker pool. */
    public void shutdown() {
        Thread joiningAcceptThread;
        synchronized (this) {
            if (!running && workers.isShutdown()) {
                return;
            }
            running = false;
            closeQuietly(serverSocket);
            serverSocket = null;
            joiningAcceptThread = acceptThread;
        }

        for (Socket client : clients) {
            closeQuietly(client);
        }
        clients.clear();

        workers.shutdown();
        try {
            if (!workers.awaitTermination(3, TimeUnit.SECONDS)) {
                workers.shutdownNow();
            }
        } catch (InterruptedException ex) {
            workers.shutdownNow();
            Thread.currentThread().interrupt();
        }

        if (joiningAcceptThread != null && joiningAcceptThread != Thread.currentThread()) {
            try {
                joiningAcceptThread.join(1000);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
        }
        LOG.info("Mini Redis server stopped");
    }

    /** Blocks the calling thread until the accept loop exits. */
    public void awaitTermination() {
        Thread thread = acceptThread;
        if (thread == null) {
            return;
        }
        try {
            thread.join();
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void close() {
        shutdown();
    }

    private static void closeQuietly(ServerSocket socket) {
        if (socket != null && !socket.isClosed()) {
            try {
                socket.close();
            } catch (IOException ignored) {
                // Nothing useful to do.
            }
        }
    }

    private static void closeQuietly(Socket socket) {
        if (socket != null && !socket.isClosed()) {
            try {
                socket.close();
            } catch (IOException ignored) {
                // Nothing useful to do.
            }
        }
    }
}
