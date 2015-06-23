/*
 * Copyright 2014 Mariam Hakobyan
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
