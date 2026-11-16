package com.miniredis.storage;

import java.util.concurrent.locks.ReentrantLock;

/**
 * A fixed array of locks indexed by key hash. Different keys can be modified
 * concurrently; the same key is always protected by the same lock. Collisions
 * only reduce parallelism, never correctness. There is intentionally no single
 * global database lock.
 */
final class KeyLockManager {

    private final ReentrantLock[] locks;

    KeyLockManager(int stripeCount) {
        if (stripeCount <= 0 || (stripeCount & (stripeCount - 1)) != 0) {
            throw new IllegalArgumentException("stripeCount must be a positive power of two");
        }
        locks = new ReentrantLock[stripeCount];
        for (int i = 0; i < stripeCount; i++) {
            locks[i] = new ReentrantLock();
        }
    }

    ReentrantLock forKey(String key) {
        int hash = key.hashCode();
        hash ^= hash >>> 16;
        return locks[hash & (locks.length - 1)];
    }
}
