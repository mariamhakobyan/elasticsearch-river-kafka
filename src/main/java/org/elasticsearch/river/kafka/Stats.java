package org.elasticsearch.river.kafka;

public class Stats {
    public int failed;
    public int succeeded;
    public int flushCount;
    public int messagesReceived;

    public Stats() {
        resetStats();
    }
    
    public void resetStats() {
        failed = 0;
        succeeded = 0;
        flushCount = 0;
        messagesReceived = 0;
    }

    @Override
    public String toString() {
        return String.format(
                "[failed=%s, succeeded=%s, flushCount=%s, messagesReceived=%s]",
                failed, succeeded, flushCount, messagesReceived);
    }
}
