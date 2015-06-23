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
