package com.miniredis.storage;

import com.miniredis.persistence.MutationLog;
import com.miniredis.persistence.PersistenceException;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Thread-safe in-memory data store.
 *
 * <p>All operations on one key take a striped lock. The lock protects compound
 * operations (INCR, LPUSH, RPOP, SADD, expiry) as well as collection mutation,
 * while operations on different keys run in parallel. ConcurrentHashMap is used
 * for lock-free snapshots of key existence and for scalable lookup.</p>
 *
 * <p>Disk ordering is write-ahead: the canonical mutation is flushed to the AOF
 * before the in-memory change is published and before the client reply is sent.
 * AOF failures are surfaced as {@link PersistenceException}.</p>
 */
public final class Database {

    private static final Logger LOG = Logger.getLogger(Database.class.getName());
    private static final long NO_EXPIRY = 0L;

    private final ConcurrentHashMap<String, Entry> data = new ConcurrentHashMap<>();
    private final KeyLockManager keyLocks;
    private final DelayQueue<ExpiryTask> expiryQueue = new DelayQueue<>();
    private volatile MutationLog mutationLog = MutationLog.NOOP;

    public Database() {
        this(256);
    }

    Database(int stripeCount) {
        this.keyLocks = new KeyLockManager(stripeCount);
    }

    /**
     * Installs the durable log. Called after replay has finished, so recovered
     * commands are never written back to the file they came from.
     */
    public void setMutationLog(MutationLog mutationLog) {
        this.mutationLog = Objects.requireNonNull(mutationLog, "mutationLog");
    }

    public String get(String key) {
        ReentrantLock lock = keyLocks.forKey(key);
        lock.lock();
        try {
            Entry entry = liveEntry(key);
            if (entry == null) {
                return null;
            }
            requireType(entry, ValueType.STRING);
            return (String) entry.value;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Stores a string, replacing any previous value and TTL. An absolute
     * expiration at or before now behaves as a deletion (used during replay).
     */
    public void set(String key, String value, long expiresAtMillis) {
        ReentrantLock lock = keyLocks.forKey(key);
        lock.lock();
        try {
            long now = System.currentTimeMillis();
            if (expiresAtMillis != NO_EXPIRY && expiresAtMillis <= now) {
                data.remove(key);
                return;
            }
            append(expiresAtMillis == NO_EXPIRY
                    ? List.of("SET", key, value)
                    : List.of("SET", key, value, "PXAT", Long.toString(expiresAtMillis)));
            data.put(key, new Entry(ValueType.STRING, value, expiresAtMillis));
            if (expiresAtMillis != NO_EXPIRY) {
                expiryQueue.offer(new ExpiryTask(key, expiresAtMillis));
            }
        } finally {
            lock.unlock();
        }
    }

    public boolean delete(String key) {
        ReentrantLock lock = keyLocks.forKey(key);
        lock.lock();
        try {
            Entry entry = liveEntry(key);
            if (entry == null) {
                return false;
            }
            append(List.of("DEL", key));
            data.remove(key);
            return true;
        } finally {
            lock.unlock();
        }
    }

    public boolean exists(String key) {
        ReentrantLock lock = keyLocks.forKey(key);
        lock.lock();
        try {
            return liveEntry(key) != null;
        } finally {
            lock.unlock();
        }
    }

    public List<String> keys() {
        List<String> result = new ArrayList<>();
        long now = System.currentTimeMillis();
        for (var entry : data.entrySet()) {
            Entry value = entry.getValue();
            // Filtering is intentionally read-only. Removing here without the
            // key lock could race with a mutation that already validated the
            // entry. The background sweeper performs deletion under the lock.
            if (value.expiresAtMillis == NO_EXPIRY || value.expiresAtMillis > now) {
                result.add(entry.getKey());
            }
        }
        result.sort(String::compareTo);
        return result;
    }

    /** Redis-style TTL: -2 missing, -1 persistent, otherwise seconds rounded up. */
    public long ttl(String key) {
        ReentrantLock lock = keyLocks.forKey(key);
        lock.lock();
        try {
            Entry entry = liveEntry(key);
            if (entry == null) {
                return -2;
            }
            if (entry.expiresAtMillis == NO_EXPIRY) {
                return -1;
            }
            long remaining = entry.expiresAtMillis - System.currentTimeMillis();
            if (remaining <= 0) {
                return -2;
            }
            return (remaining + 999) / 1000;
        } finally {
            lock.unlock();
        }
    }

    public boolean persist(String key) {
        ReentrantLock lock = keyLocks.forKey(key);
        lock.lock();
        try {
            Entry entry = liveEntry(key);
            if (entry == null || entry.expiresAtMillis == NO_EXPIRY) {
                return false;
            }
            append(List.of("PERSIST", key));
            entry.expiresAtMillis = NO_EXPIRY;
            return true;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Atomically adds delta to a numeric string. The TTL is preserved. The log
     * records the resulting value with an absolute expiry, which makes recovery
     * independent of the counter arithmetic.
     */
    public long increment(String key, long delta) {
        ReentrantLock lock = keyLocks.forKey(key);
        lock.lock();
        try {
            Entry entry = liveEntry(key);
            long current = 0L;
            long expiresAt = NO_EXPIRY;
            if (entry != null) {
                requireType(entry, ValueType.STRING);
                try {
                    current = Long.parseLong((String) entry.value);
                } catch (NumberFormatException ex) {
                    throw new InvalidValueException("value is not an integer or out of range");
                }
                expiresAt = entry.expiresAtMillis;
            }

            long result;
            try {
                result = Math.addExact(current, delta);
            } catch (ArithmeticException ex) {
                throw new InvalidValueException("increment or decrement would overflow");
            }

            String newValue = Long.toString(result);
            append(expiresAt == NO_EXPIRY
                    ? List.of("SET", key, newValue)
                    : List.of("SET", key, newValue, "PXAT", Long.toString(expiresAt)));
            data.put(key, new Entry(ValueType.STRING, newValue, expiresAt));
            return result;
        } finally {
            lock.unlock();
        }
    }

    public long pushLeft(String key, String value) {
        ReentrantLock lock = keyLocks.forKey(key);
        lock.lock();
        try {
            Entry entry = liveEntry(key);
            if (entry == null) {
                append(List.of("LPUSH", key, value));
                Deque<String> list = new ArrayDeque<>();
                list.addFirst(value);
                data.put(key, new Entry(ValueType.LIST, list, NO_EXPIRY));
                return 1;
            }
            requireType(entry, ValueType.LIST);
            @SuppressWarnings("unchecked")
            Deque<String> list = (Deque<String>) entry.value;
            append(List.of("LPUSH", key, value));
            list.addFirst(value);
            return list.size();
        } finally {
            lock.unlock();
        }
    }

    public String popRight(String key) {
        ReentrantLock lock = keyLocks.forKey(key);
        lock.lock();
        try {
            Entry entry = liveEntry(key);
            if (entry == null) {
                return null;
            }
            requireType(entry, ValueType.LIST);
            @SuppressWarnings("unchecked")
            Deque<String> list = (Deque<String>) entry.value;
            if (list.isEmpty()) {
                data.remove(key, entry);
                return null;
            }
            String value = list.peekLast();
            append(List.of("RPOP", key));
            list.removeLast();
            if (list.isEmpty()) {
                data.remove(key, entry);
            }
            return value;
        } finally {
            lock.unlock();
        }
    }

    public long listLength(String key) {
        ReentrantLock lock = keyLocks.forKey(key);
        lock.lock();
        try {
            Entry entry = liveEntry(key);
            if (entry == null) {
                return 0;
            }
            requireType(entry, ValueType.LIST);
            return ((Deque<?>) entry.value).size();
        } finally {
            lock.unlock();
        }
    }

    public long addToSet(String key, String member) {
        ReentrantLock lock = keyLocks.forKey(key);
        lock.lock();
        try {
            Entry entry = liveEntry(key);
            if (entry == null) {
                append(List.of("SADD", key, member));
                Set<String> set = new LinkedHashSet<>();
                set.add(member);
                data.put(key, new Entry(ValueType.SET, set, NO_EXPIRY));
                return 1;
            }
            requireType(entry, ValueType.SET);
            @SuppressWarnings("unchecked")
            Set<String> set = (Set<String>) entry.value;
            if (set.contains(member)) {
                return 0;
            }
            append(List.of("SADD", key, member));
            set.add(member);
            return 1;
        } finally {
            lock.unlock();
        }
    }

    public boolean isSetMember(String key, String member) {
        ReentrantLock lock = keyLocks.forKey(key);
        lock.lock();
        try {
            Entry entry = liveEntry(key);
            if (entry == null) {
                return false;
            }
            requireType(entry, ValueType.SET);
            return ((Set<?>) entry.value).contains(member);
        } finally {
            lock.unlock();
        }
    }

    public long setSize(String key) {
        ReentrantLock lock = keyLocks.forKey(key);
        lock.lock();
        try {
            Entry entry = liveEntry(key);
            if (entry == null) {
                return 0;
            }
            requireType(entry, ValueType.SET);
            return ((Set<?>) entry.value).size();
        } finally {
            lock.unlock();
        }
    }

    /** Used by the background expiry thread. */
    public ExpiryTask pollExpiry(long timeoutMillis) {
        try {
            return expiryQueue.poll(timeoutMillis, TimeUnit.MILLISECONDS);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            return null;
        }
    }

    /**
     * Removes a key if the task still refers to its current absolute expiry.
     * Old tasks left behind by SET/PERSIST are ignored. A DEL is logged so the
     * on-disk log does not grow forever with dead keys.
     */
    public boolean expireIfDue(ExpiryTask task) {
        ReentrantLock lock = keyLocks.forKey(task.key());
        lock.lock();
        try {
            Entry entry = data.get(task.key());
            long now = System.currentTimeMillis();
            if (entry == null
                    || entry.expiresAtMillis != task.expiresAtMillis()
                    || entry.expiresAtMillis > now) {
                return false;
            }
            try {
                mutationLog.append(List.of("DEL", task.key()));
            } catch (IOException ex) {
                // The SET PXAT record already encodes the expiry, so replay is
                // still correct even if this best-effort cleanup cannot be logged.
                LOG.log(Level.WARNING, "Could not log expired key deletion", ex);
            }
            data.remove(task.key(), entry);
            return true;
        } finally {
            lock.unlock();
        }
    }

    /** Caller must hold the key's lock. */
    private Entry liveEntry(String key) {
        Entry entry = data.get(key);
        if (entry != null
                && entry.expiresAtMillis != NO_EXPIRY
                && entry.expiresAtMillis <= System.currentTimeMillis()) {
            data.remove(key, entry);
            return null;
        }
        return entry;
    }

    private static void requireType(Entry entry, ValueType expected) {
        if (entry.type != expected) {
            throw new WrongTypeException();
        }
    }

    private void append(List<String> command) {
        try {
            mutationLog.append(command);
        } catch (IOException ex) {
            throw new PersistenceException(ex.getMessage(), ex);
        }
    }
}
