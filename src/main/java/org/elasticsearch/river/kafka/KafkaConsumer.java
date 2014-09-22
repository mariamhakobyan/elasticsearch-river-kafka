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

import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import org.elasticsearch.common.logging.ESLogger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Kafka consumer, written using Kafka Consumer Group (High Level) API, opens kafka streams to be able to consume messages.
 *
 * @author Mariam Hakobyan
 */
public class KafkaConsumer {

    private final static Integer AMOUNT_OF_THREADS_PER_CONSUMER = 1;
    private final static String GROUP_ID = "elasticsearch-kafka-river";
    private final static Integer CONSUMER_TIMEOUT = 15000;

    private RiverProperties riverProperties;

    private List<KafkaStream<byte[], byte[]>> streams;
    private ConsumerConnector consumer;


    public KafkaConsumer(final RiverProperties riverProperties, final ESLogger logger) {
        this.riverProperties = riverProperties;

        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(createConsumerConfig(riverProperties));

        final Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(riverProperties.getTopic(), AMOUNT_OF_THREADS_PER_CONSUMER);

        final Map<String, List<KafkaStream<byte[], byte[]>>> consumerStreams = consumer.createMessageStreams(topicCountMap);

        streams = consumerStreams.get(riverProperties.getTopic());

        logger.info("Started kafka consumer for topic: " + riverProperties.getTopic() + " with " + streams.size() + " partitions in it.");
    }

    private ConsumerConfig createConsumerConfig(final RiverProperties riverProperties) {
        final Properties props = new Properties();
        props.put(RiverProperties.ZOOKEEPER_CONNECT, riverProperties.getZookeeperConnect());
        props.put("zookeeper.connection.timeout.ms", String.valueOf(riverProperties.getZookeeperConnectionTimeout()));
        props.put("group.id", GROUP_ID);
        props.put("auto.commit.enable", String.valueOf(false));
        props.put("consumer.timeout.ms", String.valueOf(CONSUMER_TIMEOUT));

        return new ConsumerConfig(props);
    }


    List<KafkaStream<byte[], byte[]>> getStreams() {
        return streams;
    }

    RiverProperties getRiverProperties() {
        return riverProperties;
    }

    ConsumerConnector getConsumer() {
        return consumer;
    }
}
