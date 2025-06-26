package com.dimi.producers;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

public class ThroughputThrottler {

    private static final long NS_PER_MS = 1000000L;
    private static final long NS_PER_SEC = 1000 * NS_PER_MS;
    private static final long MIN_SLEEP_NS = 2 * NS_PER_MS;

    private final long startMs;
    private final Supplier<AtomicInteger> targetThroughputSupplier;

    private long sleepDeficitNs;
    private boolean wakeup;

    public ThroughputThrottler(Supplier<AtomicInteger> rateSupplier, long startMs) {
        this.startMs = startMs;
        this.targetThroughputSupplier = rateSupplier;
    }

    private int getTargetThroughput() {
        AtomicInteger value = targetThroughputSupplier.get();
        return value != null ? value.get() : 0;
    }

    public boolean shouldThrottle(long amountSoFar, long sendStartMs) {
        int targetThroughput = getTargetThroughput();
        if (targetThroughput <= 0) {
            return true; // Pause completely or skip throttle
        }

        float elapsedSec = (sendStartMs - startMs) / 1000.f;
        return elapsedSec > 0 && ((double) amountSoFar / elapsedSec) > targetThroughput;
    }

    public void throttle() {
        int targetThroughput = getTargetThroughput();
        if (targetThroughput <= 0) {
            try {
                synchronized (this) {
                    while (!wakeup) {
                        this.wait();
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return;
        }

        long sleepTimeNs = (long) (NS_PER_SEC / targetThroughput);
        sleepDeficitNs += sleepTimeNs;

        if (sleepDeficitNs >= MIN_SLEEP_NS) {
            long sleepStartNs = System.nanoTime();
            try {
                synchronized (this) {
                    long remaining = sleepDeficitNs;
                    while (!wakeup && remaining > 0) {
                        long sleepMs = remaining / 1000000;
                        long sleepNs = remaining % 1000000;
                        this.wait(sleepMs, (int) sleepNs);
                        long elapsed = System.nanoTime() - sleepStartNs;
                        remaining = sleepDeficitNs - elapsed;
                    }
                    wakeup = false;
                }
                sleepDeficitNs = 0;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                long sleepElapsedNs = System.nanoTime() - sleepStartNs;
                if (sleepElapsedNs <= sleepDeficitNs) {
                    sleepDeficitNs -= sleepElapsedNs;
                }
            }
        }
    }
}
