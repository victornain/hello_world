package com.pfizer.bridge.amqtosqs;

import com.pfizer.bridge.common.BridgeRunner;

import java.util.List;

/** Runs each site bridge on its own thread so broker failures remain isolated. */
final class MultiSiteBridge implements BridgeRunner.Bridge {

    private final List<AmqToSqsBridge> bridges;
    private final List<Thread> workers;

    MultiSiteBridge(List<AmqToSqsBridge> bridges) {
        this.bridges = List.copyOf(bridges);
        this.workers = this.bridges.stream()
                .map(bridge -> new Thread(bridge::run, "amq-to-sqs-" + workerName(bridge)))
                .toList();
    }

    @Override
    public void run() {
        workers.forEach(Thread::start);
        for (Thread worker : workers) {
            try {
                worker.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                stop();
                return;
            }
        }
    }

    @Override
    public void stop() {
        bridges.forEach(AmqToSqsBridge::stop);
        workers.forEach(Thread::interrupt);
    }

    private static String workerName(AmqToSqsBridge bridge) {
        return bridge.siteId().replaceAll("[^A-Za-z0-9._-]", "-");
    }
}