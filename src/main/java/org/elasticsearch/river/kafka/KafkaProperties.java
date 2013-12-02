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

import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.river.RiverSettings;

import java.util.Map;
import java.util.Properties;


public class KafkaProperties extends Properties {

    private static final String BROKER_HOST = "brokerHost";
    private static final String BROKER_PORT = "brokerPort";
    private static final String TOPIC = "topic";
    private static final String PARTITION = "partition";

    private static final String DEFAULT_KAFKA_SERVER_URL = "localhost";
    private static final Integer DEFAULT_KAFKA_SERVER_PORT = 9092;
    private static final String DEFAULT_TOPIC = "default-topic";
    private static final Integer DEFAULT_PARTITION = 0;

    private String topic;
    private String brokerHost;
    private Integer brokerPort;
    private Integer partition;
    private Integer maxSizeOfFetchMessages = 100;


    public KafkaProperties(RiverSettings riverSettings) {

        if (riverSettings.settings().containsKey("kafka")) {
            Map<String, Object> kafkaSettings = (Map<String, Object>) riverSettings.settings().get("kafka");

            topic = (String) kafkaSettings.get("topic");
            brokerHost = XContentMapValues.nodeStringValue(kafkaSettings.get(BROKER_HOST), DEFAULT_KAFKA_SERVER_URL);
            brokerPort = XContentMapValues.nodeIntegerValue(kafkaSettings.get(BROKER_PORT), DEFAULT_KAFKA_SERVER_PORT);
            partition = XContentMapValues.nodeIntegerValue(kafkaSettings.get("partition"), DEFAULT_PARTITION);

            this.setProperty(TOPIC, topic);
            this.setProperty(BROKER_HOST, brokerHost);
            this.setProperty(BROKER_PORT, String.valueOf(brokerPort));
            this.setProperty(PARTITION, String.valueOf(partition));

        } else {
            // Use the default properties
            brokerHost = DEFAULT_KAFKA_SERVER_URL;
            brokerPort = DEFAULT_KAFKA_SERVER_PORT;
            topic = DEFAULT_TOPIC;
            partition = DEFAULT_PARTITION;
        }
    }

    public String getTopic() {
        return topic;
    }

    public String getBrokerHost() {
        return brokerHost;
    }

    public Integer getBrokerPort() {
        return brokerPort;
    }

    public Integer getPartition() {
        return partition;
    }

    public Integer getMaxSizeOfFetchMessages() {
        return maxSizeOfFetchMessages;
    }
}
