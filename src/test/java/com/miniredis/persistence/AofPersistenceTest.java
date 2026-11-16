package com.miniredis.persistence;

import com.miniredis.storage.Database;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AofPersistenceTest {

    @TempDir
    Path tempDir;

    private Path aof() {
        return tempDir.resolve("appendonly.aof");
    }

    private Store open() throws IOException {
        return open(AofManager.FsyncPolicy.NO);
    }

    private Store open(AofManager.FsyncPolicy policy) throws IOException {
        Database db = new Database();
        AofManager aof = new AofManager(aof(), policy);
        aof.load(db);
        db.setMutationLog(aof);
        return new Store(db, aof);
    }

    @Test
    void stringsAndCountersRecoverAfterRestart() throws Exception {
        try (Store store = open()) {
            store.db.set("name", "Rahul", 0);
            store.db.increment("visits", 1);
            store.db.increment("visits", 1);
            store.db.increment("visits", 1);
        }

        try (Store store = open()) {
            assertEquals("Rahul", store.db.get("name"));
            assertEquals("3", store.db.get("visits"));
            assertEquals(4, store.db.increment("visits", 1));
        }
    }

    @Test
    void listsSetsAndDeletionsRecoverAfterRestart() throws Exception {
        try (Store store = open()) {
            store.db.pushLeft("letters", "a");
            store.db.pushLeft("letters", "b");
            store.db.pushLeft("letters", "c");
            assertEquals("a", store.db.popRight("letters"));
            store.db.pushLeft("letters", "d");
            store.db.addToSet("tags", "java");
            store.db.addToSet("tags", "redis");
            store.db.set("deleted", "value", 0);
            store.db.delete("deleted");
        }

        try (Store store = open()) {
            assertEquals(3, store.db.listLength("letters"));
            assertEquals("b", store.db.popRight("letters"));
            assertEquals("c", store.db.popRight("letters"));
            assertEquals("d", store.db.popRight("letters"));
            assertEquals(2, store.db.setSize("tags"));
            assertTrue(store.db.isSetMember("tags", "java"));
            assertFalse(store.db.exists("deleted"));
        }
    }

    @Test
    void expirationTimesAndExpiredKeysSurviveRestartCorrectly() throws Exception {
        try (Store store = open()) {
            store.db.set("future", "value", System.currentTimeMillis() + 60_000);
            store.db.set("short", "value", System.currentTimeMillis() + 100);
            Thread.sleep(200);
        }

        try (Store store = open()) {
            assertNull(store.db.get("short"));
            assertFalse(store.db.exists("short"));
            assertEquals("value", store.db.get("future"));
            long ttl = store.db.ttl("future");
            assertTrue(ttl > 0 && ttl <= 60, "ttl=" + ttl);

            assertTrue(store.db.persist("future"));
            assertEquals(-1, store.db.ttl("future"));
        }

        try (Store store = open()) {
            assertEquals(-1, store.db.ttl("future"), "PERSIST must survive replay");
        }
    }

    @Test
    void unusualCharactersAreFramedSafely() throws Exception {
        String value = "hello world \"quoted\" line1\nline2\tend";
        try (Store store = open()) {
            store.db.set("weird key with spaces", value, 0);
        }
        try (Store store = open()) {
            assertEquals(value, store.db.get("weird key with spaces"));
        }
    }

    @Test
    void incompleteTrailingRecordIsIgnoredAndTruncated() throws Exception {
        long validLength;
        try (Store store = open()) {
            store.db.set("a", "1", 0);
            store.db.set("b", "2", 0);
        }
        validLength = Files.size(aof());
        Files.write(aof(), new byte[]{1, 2, 3, 4, 5, 6, 7}, StandardOpenOption.APPEND);

        Database db = new Database();
        AofManager aof = new AofManager(aof(), AofManager.FsyncPolicy.NO);
        AofManager.ReplayStats stats = aof.load(db);
        db.setMutationLog(aof);

        assertEquals(2, stats.recordsApplied());
        assertEquals(7, stats.bytesTruncated());
        assertEquals("1", db.get("a"));
        assertEquals("2", db.get("b"));
        assertEquals(validLength, Files.size(aof()));
        aof.close();
    }

    @Test
    void corruptRecordStopsAtLastConsistentPrefix() throws Exception {
        try (Store store = open()) {
            store.db.set("first", "safe", 0);
            store.db.set("second", "corrupt-me", 0);
        }
        byte[] bytes = Files.readAllBytes(aof());
        bytes[bytes.length - 1] ^= 0x5A; // break the CRC of the second record
        Files.write(aof(), bytes, StandardOpenOption.TRUNCATE_EXISTING);

        Database db = new Database();
        AofManager aof = new AofManager(aof(), AofManager.FsyncPolicy.NO);
        AofManager.ReplayStats stats = aof.load(db);
        db.setMutationLog(aof);

        assertEquals(1, stats.recordsApplied());
        assertTrue(stats.bytesTruncated() > 0);
        assertEquals("safe", db.get("first"));
        assertNull(db.get("second"));
        aof.close();
    }

    @Test
    void malformedLengthDoesNotCrashStartup() throws Exception {
        try (Store store = open()) {
            store.db.set("a", "1", 0);
        }
        byte[] badHeader = new byte[]{
                0x7F, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, 0, 0, 0, 0
        };
        Files.write(aof(), badHeader, StandardOpenOption.APPEND);

        Database db = new Database();
        AofManager aof = new AofManager(aof(), AofManager.FsyncPolicy.NO);
        AofManager.ReplayStats stats = aof.load(db);
        db.setMutationLog(aof);

        assertEquals(1, stats.recordsApplied());
        assertEquals("1", db.get("a"));
        assertTrue(stats.bytesTruncated() >= badHeader.length);
        aof.close();
    }

    @Test
    @Timeout(30)
    void concurrentMutationsRecoverToTheInMemoryState() throws Exception {
        int threads = 20;
        int increments = 100;
        try (Store store = open()) {
            ExecutorService pool = Executors.newFixedThreadPool(threads);
            CountDownLatch start = new CountDownLatch(1);
            CountDownLatch done = new CountDownLatch(threads);
            AtomicReference<Throwable> failure = new AtomicReference<>();

            for (int t = 0; t < threads; t++) {
                pool.submit(() -> {
                    try {
                        start.await();
                        for (int i = 0; i < increments; i++) {
                            store.db.increment("shared", 1);
                        }
                    } catch (Throwable ex) {
                        failure.compareAndSet(null, ex);
                    } finally {
                        done.countDown();
                    }
                });
            }
            start.countDown();
            assertTrue(done.await(20, TimeUnit.SECONDS));
            pool.shutdownNow();
            assertNull(failure.get());
            assertEquals(Integer.toString(threads * increments), store.db.get("shared"));
        }

        try (Store store = open()) {
            assertEquals(Integer.toString(threads * increments), store.db.get("shared"));
        }
    }

    @Test
    void writesAfterAofFailureDoNotChangeMemory() throws Exception {
        Database db = new Database();
        AofManager aof = new AofManager(aof(), AofManager.FsyncPolicy.NO);
        aof.load(db);
        db.setMutationLog(aof);
        db.set("a", "1", 0);
        aof.close();

        assertThrows(PersistenceException.class, () -> db.set("b", "2", 0));
        assertNull(db.get("b"));
        assertEquals("1", db.get("a"));
    }

    private static final class Store implements AutoCloseable {
        final Database db;
        final AofManager aof;

        Store(Database db, AofManager aof) {
            this.db = db;
            this.aof = aof;
        }

        @Override
        public void close() {
            aof.close();
        }
    }
}
