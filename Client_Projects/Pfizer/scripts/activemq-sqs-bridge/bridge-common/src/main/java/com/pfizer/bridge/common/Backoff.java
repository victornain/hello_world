package com.pfizer.bridge.common;

import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BooleanSupplier;

/** Exponential backoff with jitter. Not thread-safe; use one instance per loop. */
public final class Backoff {

    private final long initialMs;
    private final long maxMs;
    private long currentMs;

    public Backoff(long initialMs, long maxMs) {
        this.initialMs = initialMs;
        this.maxMs = maxMs;
        this.currentMs = initialMs;
    }

    public static Backoff fromEnv(EnvConfig env) {
        return new Backoff(env.longValue("RETRY_INITIAL_DELAY_MS", 1000), env.longValue("RETRY_MAX_DELAY_MS", 60000));
    }

    public long nextDelayMs() {
        long delay = currentMs + ThreadLocalRandom.current().nextLong(currentMs / 4 + 1);
        currentMs = Math.min(currentMs * 2, maxMs);
        return Math.min(delay, maxMs);
    }

    public void reset() {
        currentMs = initialMs;
    }

    /** @return false if interrupted (caller should stop). */
    public static boolean sleep(long ms) {
        try {
            Thread.sleep(ms);
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    /**
     * Sleeps up to {@code ms} in short slices, returning early once {@code keepGoing} turns false,
     * so long waits (e.g. a 10-minute poll interval) do not delay shutdown. @return false if interrupted.
     */
    public static boolean sleepWhile(long ms, BooleanSupplier keepGoing) {
        long deadline = System.currentTimeMillis() + ms;
        while (keepGoing.getAsBoolean()) {
            long remaining = deadline - System.currentTimeMillis();
            if (remaining <= 0) {
                return true;
            }
            if (!sleep(Math.min(remaining, 500))) {
                return false;
            }
        }
        return true;
    }
}
