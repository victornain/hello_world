package com.pfizer.bridge.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Runs a bridge loop on the main thread and stops it cleanly on SIGTERM/SIGINT (docker stop). */
public final class BridgeRunner {

    private static final Logger LOG = LoggerFactory.getLogger(BridgeRunner.class);

    public interface Bridge {
        void run();

        void stop();
    }

    private BridgeRunner() {
    }

    public static void run(Bridge bridge, long shutdownGraceMs) {
        Thread main = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Shutdown requested, finishing in-flight work...");
            bridge.stop();
            try {
                main.join(shutdownGraceMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, "shutdown-hook"));
        bridge.run();
        LOG.info("Bridge stopped.");
    }
}
