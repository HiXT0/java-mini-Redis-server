package com.miniredis.storage;

/**
 * One key's value. Strings are immutable. Lists and sets are mutable but are
 * only touched while the key's stripe lock is held.
 */
final class Entry {

    final ValueType type;
    final Object value;
    volatile long expiresAtMillis; // 0 means "no expiration"

    Entry(ValueType type, Object value, long expiresAtMillis) {
        this.type = type;
        this.value = value;
        this.expiresAtMillis = expiresAtMillis;
    }
}
