package com.miniredis.expiry;

import com.miniredis.storage.Database;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ExpirationTest {

    @Test
    @Timeout(5)
    void backgroundCleanupRemovesExpiredKey() throws Exception {
        Database db = new Database();
        try (ExpiryManager expiry = new ExpiryManager(db)) {
            expiry.start();
            db.set("temp", "value", System.currentTimeMillis() + 100);

            long deadline = System.currentTimeMillis() + 3000;
            while (db.exists("temp") && System.currentTimeMillis() < deadline) {
                Thread.sleep(20);
            }
            assertFalse(db.exists("temp"));
            assertNull(db.get("temp"));
            assertFalse(db.keys().contains("temp"));
        }
    }

    @Test
    void ttlCountsDownAndReturnsSpecialValues() throws Exception {
        Database db = new Database();
        db.set("token", "abc", System.currentTimeMillis() + 1100);
        long ttl = db.ttl("token");
        assertTrue(ttl >= 1 && ttl <= 2, "unexpected ttl: " + ttl);

        Thread.sleep(1200);
        assertEquals(-2, db.ttl("token"));
        assertNull(db.get("token"));

        db.set("persistent", "x", 0);
        assertEquals(-1, db.ttl("persistent"));
        assertEquals(-2, db.ttl("missing"));
    }

    @Test
    void overwritingExpiringKeyClearsOldTtl() throws Exception {
        Database db = new Database();
        db.set("k", "one", System.currentTimeMillis() + 80);
        db.set("k", "two", 0);
        Thread.sleep(150);
        assertTrue(db.exists("k"));
        assertEquals("two", db.get("k"));
        assertEquals(-1, db.ttl("k"));
    }

    @Test
    void persistRemovesExpiration() throws Exception {
        Database db = new Database();
        db.set("k", "v", System.currentTimeMillis() + 80);
        assertTrue(db.persist("k"));
        Thread.sleep(150);
        assertTrue(db.exists("k"));
        assertEquals("v", db.get("k"));
    }

    @Test
    @Timeout(15)
    void concurrentExpiryAndUpdatesRemainSafe() throws Exception {
        Database db = new Database();
        try (ExpiryManager expiry = new ExpiryManager(db)) {
            expiry.start();
            int threads = 12;
            ExecutorService pool = Executors.newFixedThreadPool(threads);
            CountDownLatch start = new CountDownLatch(1);
            CountDownLatch done = new CountDownLatch(threads);
            AtomicReference<Throwable> failure = new AtomicReference<>();

            for (int t = 0; t < threads; t++) {
                final int id = t;
                pool.submit(() -> {
                    try {
                        start.await();
                        for (int i = 0; i < 500; i++) {
                            String key = "key-" + (i % 5);
                            if (id % 2 == 0) {
                                db.set(key, "v-" + i, System.currentTimeMillis() + 20);
                            } else {
                                db.get(key);
                                db.exists(key);
                                db.keys();
                            }
                        }
                    } catch (Throwable ex) {
                        failure.compareAndSet(null, ex);
                    } finally {
                        done.countDown();
                    }
                });
            }

            start.countDown();
            assertTrue(done.await(12, TimeUnit.SECONDS));
            pool.shutdownNow();
            assertNull(failure.get(), () -> "concurrent expiration failure: " + failure.get());
        }
    }
}
