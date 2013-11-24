/*
 * Copyright 2013 Mariam Hakobyan
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

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.TopicAndPartition;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import org.elasticsearch.common.logging.ESLogger;

import java.util.HashMap;
import java.util.Map;


public class KafkaConsumer {

    private KafkaProperties kafkaProperties;
    private SimpleConsumer kafkaConsumer;
    private final String clientName;
    private Long currentOffset;

    private ESLogger logger;

    public KafkaConsumer(KafkaProperties kafkaProperties, ESLogger logger) {

        this.kafkaProperties = kafkaProperties;
        this.logger = logger;

        clientName = "Client_" + kafkaProperties.getTopic() + "_" + kafkaProperties.getPartition();

        kafkaConsumer = new SimpleConsumer(kafkaProperties.getBrokerHost(),
                kafkaProperties.getBrokerPort(),
                1000, 1024 * 1024 * 10, clientName);    // broker host, broker port, soTimeout, bufferSize, clientname

        this.currentOffset = getCurrentOffset(kafkaConsumer, kafkaProperties.getTopic(), kafkaProperties.getPartition(),
                kafka.api.OffsetRequest.EarliestTime(), clientName);
    }

    public ByteBufferMessageSet readMessagesFromKafka() {
        String topic = kafkaProperties.getTopic();
        Integer partition = kafkaProperties.getPartition();

        FetchRequest req = new FetchRequestBuilder()
                .clientId(clientName)
                .addFetch(topic, partition, currentOffset, kafkaProperties.getMaxSizeOfFetchMessages())   // topic, partition, offset, maxSize
                .build();
        return kafkaConsumer.fetch(req).messageSet(topic, partition);  // topic, partition
    }

    /**
     * Defines where to start reading data from
     * Helpers Available:
     * kafka.api.OffsetRequest.EarliestTime() => finds the beginning of the data in the logs and starts streaming
     * from there
     * kafka.api.OffsetRequest.LatestTime()   => will only stream new messages
     *
     * @param consumer
     * @param topic
     * @param partition
     * @param whichTime
     * @param clientName
     * @return
     */
    public long getCurrentOffset(SimpleConsumer consumer, String topic, int partition, long whichTime, String clientName) {
        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
        Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
        requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(whichTime, 1));
        kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(
                requestInfo, kafka.api.OffsetRequest.CurrentVersion(), clientName);
        OffsetResponse response = consumer.getOffsetsBefore(request);

        if (response.hasError()) {
            System.out.println("Error fetching data Offset Data the Broker. Reason: " + response.errorCode(topic, partition));
            return 0;
        }
        long[] offsets = response.offsets(topic, partition);
        return offsets[0];
    }

    public KafkaProperties getKafkaProperties() {
        return kafkaProperties;
    }

    public Long getCurrentOffset() {
        return currentOffset;
    }

    public void setCurrentOffset(Long currentOffset) {
        this.currentOffset = currentOffset;
    }
}
