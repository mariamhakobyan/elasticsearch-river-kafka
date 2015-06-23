package org.elasticsearch.river.kafka;

import com.timgroup.statsd.NonBlockingStatsDClient;
import com.timgroup.statsd.StatsDClient;

public class StatsAgent {
    private StatsDClient client;
    private String keyPrefix;

    public StatsAgent(RiverConfig riverConfig) {
        
        this.client = new NonBlockingStatsDClient(
                riverConfig.getStatsdPrefix(),
                riverConfig.getStatsdHost(),
                riverConfig.getStatsdPort());
        
        this.keyPrefix = riverConfig.getTopic();
    }
    
    public void logStats(Stats stats) {
        client.count(keyPrefix + ".failed", stats.failed.get());
        client.count(keyPrefix + ".succeeded", stats.succeeded.get());
        client.count(keyPrefix + ".flushCount", stats.flushCount.get());
        client.count(keyPrefix + ".messagesReceived", stats.messagesReceived.get());

        for(Integer partitionId : stats.lastCommitOffsetByPartitionId.keySet()) {
            client.gauge(
                    String.format("%s.partition_%s.offset", keyPrefix, partitionId),
                    stats.lastCommitOffsetByPartitionId.get(partitionId));
        }
    }
}
