package com.miniredis.storage;

import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

/** A scheduled expiration check. Stale entries are ignored by Database. */
public record ExpiryTask(String key, long expiresAtMillis) implements Delayed {

    @Override
    public long getDelay(TimeUnit unit) {
        long remaining = expiresAtMillis - System.currentTimeMillis();
        return unit.convert(remaining, TimeUnit.MILLISECONDS);
    }

    @Override
    public int compareTo(Delayed other) {
        return Long.compare(getDelay(TimeUnit.MILLISECONDS), other.getDelay(TimeUnit.MILLISECONDS));
    }
}
