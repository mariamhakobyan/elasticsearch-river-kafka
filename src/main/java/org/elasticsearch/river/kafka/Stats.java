package org.elasticsearch.river.kafka;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class Stats {
    public AtomicInteger failed;
    public AtomicInteger succeeded;
    public AtomicInteger flushCount;
    public AtomicInteger messagesReceived;
    public ConcurrentHashMap<Integer, Long> lastCommitOffsetByPartitionId;

    public Stats() {
        failed = new AtomicInteger(0);
        succeeded = new AtomicInteger(0);
        flushCount = new AtomicInteger(0);
        messagesReceived = new AtomicInteger(0);
        lastCommitOffsetByPartitionId = new ConcurrentHashMap<Integer, Long>();
    }

    public Stats getCloneAndReset() {
        Stats clone = new Stats();

        clone.failed.set(failed.getAndSet(0));
        clone.succeeded.set(succeeded.getAndSet(0));
        clone.flushCount.set(flushCount.getAndSet(0));
        clone.messagesReceived.set(messagesReceived.getAndSet(0));
        
        clone.lastCommitOffsetByPartitionId.putAll(lastCommitOffsetByPartitionId);
        lastCommitOffsetByPartitionId.clear();

        return clone;
    }

    @Override
    public String toString() {
        return String.format(
                "[failed=%s, succeeded=%s, flushCount=%s, messagesReceived=%s, lastCommitOffset=%s]",
                failed.get(), succeeded.get(), flushCount.get(), messagesReceived.get(), lastCommitOffsetByPartitionId);
    }
}
