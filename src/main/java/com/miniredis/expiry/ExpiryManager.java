package com.miniredis.expiry;

import com.miniredis.storage.Database;
import com.miniredis.storage.ExpiryTask;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Single daemon thread that consumes due keys from the database's DelayQueue.
 * A dedicated thread avoids scanning the entire keyspace on a timer.
 */
public final class ExpiryManager implements AutoCloseable {

    private static final Logger LOG = Logger.getLogger(ExpiryManager.class.getName());

    private final Database database;
    private final Thread thread;
    private volatile boolean running;
    private volatile boolean started;

    public ExpiryManager(Database database) {
        this.database = database;
        this.thread = new Thread(this::run, "mini-redis-expiry");
        this.thread.setDaemon(true);
    }

    public void start() {
        if (started) {
            return;
        }
        started = true;
        running = true;
        thread.start();
    }

    private void run() {
        while (running) {
            ExpiryTask task = database.pollExpiry(200);
            if (task == null) {
                continue;
            }
            try {
                database.expireIfDue(task);
            } catch (RuntimeException ex) {
                LOG.log(Level.WARNING, "Expiry cleanup failed", ex);
            }
        }
    }

    @Override
    public void close() {
        running = false;
        if (started) {
            thread.interrupt();
            try {
                thread.join(1000);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
