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
import org.elasticsearch.common.logging.ESLoggerFactory;

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

    private List<KafkaStream<byte[], byte[]>> streams;
    private ConsumerConnector consumerConnector;

    private static final ESLogger logger = ESLoggerFactory.getLogger(KafkaConsumer.class.getName());


    public KafkaConsumer(final RiverConfig riverConfig) {
        consumerConnector = kafka.consumer.Consumer.createJavaConsumerConnector(createConsumerConfig(riverConfig));

        final Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(riverConfig.getTopic(), AMOUNT_OF_THREADS_PER_CONSUMER);

        final Map<String, List<KafkaStream<byte[], byte[]>>> consumerStreams =
                consumerConnector.createMessageStreams(topicCountMap);

        streams = consumerStreams.get(riverConfig.getTopic());

        logger.debug("Index: {}: Started kafka consumer for topic: {} with {} partitions in it.",
                riverConfig.getIndexName(), riverConfig.getTopic(), streams.size());
    }

    private ConsumerConfig createConsumerConfig(final RiverConfig riverConfig) {
        final Properties props = new Properties();
        props.put("zookeeper.connect", riverConfig.getZookeeperConnect());
        props.put("zookeeper.connection.timeout.ms", String.valueOf(riverConfig.getZookeeperConnectionTimeout()));
        props.put("group.id", GROUP_ID);
        props.put("auto.commit.enable", String.valueOf(false));
        props.put("consumer.timeout.ms", String.valueOf(CONSUMER_TIMEOUT));

        return new ConsumerConfig(props);
    }

    public void shutdown() {
        if (consumerConnector != null) {
            consumerConnector.shutdown();
        }
    }

    List<KafkaStream<byte[], byte[]>> getStreams() {
        return streams;
    }

    ConsumerConnector getConsumerConnector() {
        return consumerConnector;
    }
}
