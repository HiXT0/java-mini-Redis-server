package com.miniredis.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DatabaseConcurrencyTest {

    @Test
    @Timeout(30)
    void oneHundredThreadsIncrementSameCounterWithoutLostUpdates() throws Exception {
        Database db = new Database();
        int threads = 100;
        int incrementsPerThread = 100;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        AtomicReference<Throwable> failure = new AtomicReference<>();

        for (int i = 0; i < threads; i++) {
            pool.submit(() -> {
                try {
                    start.await();
                    for (int j = 0; j < incrementsPerThread; j++) {
                        db.increment("shared", 1);
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
        assertEquals(Integer.toString(threads * incrementsPerThread), db.get("shared"));
        assertEquals(threads * incrementsPerThread, Long.parseLong(db.get("shared")));
    }

    @Test
    @Timeout(30)
    void concurrentListPushesDoNotLoseElements() throws Exception {
        Database db = new Database();
        int threads = 50;
        int pushesPerThread = 100;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        AtomicReference<Throwable> failure = new AtomicReference<>();

        for (int t = 0; t < threads; t++) {
            final int id = t;
            pool.submit(() -> {
                try {
                    start.await();
                    for (int i = 0; i < pushesPerThread; i++) {
                        db.pushLeft("log", id + "-" + i);
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
        assertEquals(threads * pushesPerThread, db.listLength("log"));
    }

    @Test
    @Timeout(30)
    void simultaneousPushAndPopConservesElements() throws Exception {
        Database db = new Database();
        int producers = 10;
        int pushesPerProducer = 100;
        int consumers = 10;
        int total = producers * pushesPerProducer;

        ExecutorService pool = Executors.newFixedThreadPool(producers + consumers);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch producersDone = new CountDownLatch(producers);
        CountDownLatch consumersDone = new CountDownLatch(consumers);
        AtomicBoolean producerFinished = new AtomicBoolean(false);
        AtomicInteger popped = new AtomicInteger();
        AtomicReference<Throwable> failure = new AtomicReference<>();

        for (int p = 0; p < producers; p++) {
            final int id = p;
            pool.submit(() -> {
                try {
                    start.await();
                    for (int i = 0; i < pushesPerProducer; i++) {
                        db.pushLeft("queue", id + ":" + i);
                    }
                } catch (Throwable ex) {
                    failure.compareAndSet(null, ex);
                } finally {
                    producersDone.countDown();
                }
            });
        }

        for (int c = 0; c < consumers; c++) {
            pool.submit(() -> {
                try {
                    start.await();
                    while (!producerFinished.get() || db.listLength("queue") > 0) {
                        String value = db.popRight("queue");
                        if (value != null) {
                            popped.incrementAndGet();
                        } else {
                            Thread.onSpinWait();
                        }
                    }
                } catch (Throwable ex) {
                    failure.compareAndSet(null, ex);
                } finally {
                    consumersDone.countDown();
                }
            });
        }

        start.countDown();
        assertTrue(producersDone.await(20, TimeUnit.SECONDS));
        producerFinished.set(true);
        assertTrue(consumersDone.await(20, TimeUnit.SECONDS));
        pool.shutdownNow();

        assertNull(failure.get());
        assertEquals(total, popped.get());
        assertEquals(0, db.listLength("queue"));
    }

    @Test
    @Timeout(30)
    void simultaneousSetAdditionsAreAtomic() throws Exception {
        Database db = new Database();
        int threads = 50;
        int membersPerThread = 50;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        AtomicReference<Throwable> failure = new AtomicReference<>();

        for (int t = 0; t < threads; t++) {
            final int id = t;
            pool.submit(() -> {
                try {
                    start.await();
                    for (int i = 0; i < membersPerThread; i++) {
                        db.addToSet("members", id + "-" + i);
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
        assertEquals(threads * membersPerThread, db.setSize("members"));
    }

    @Test
    @Timeout(30)
    void concurrentReadsAndWritesAreSafe() throws Exception {
        Database db = new Database();
        int writers = 20;
        int readers = 20;
        ExecutorService pool = Executors.newFixedThreadPool(writers + readers);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(writers + readers);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        List<String> allowedFinalValues = new ArrayList<>();

        for (int w = 0; w < writers; w++) {
            final String value = "writer-" + w;
            allowedFinalValues.add(value);
            pool.submit(() -> {
                try {
                    start.await();
                    for (int i = 0; i < 500; i++) {
                        db.set("hot-key", value, 0);
                    }
                } catch (Throwable ex) {
                    failure.compareAndSet(null, ex);
                } finally {
                    done.countDown();
                }
            });
        }

        for (int r = 0; r < readers; r++) {
            pool.submit(() -> {
                try {
                    start.await();
                    for (int i = 0; i < 1000; i++) {
                        db.get("hot-key");
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
        assertTrue(allowedFinalValues.contains(db.get("hot-key")));
    }
}
