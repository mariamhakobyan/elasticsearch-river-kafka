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

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;


public class KafkaProperties extends Properties {

    private static final String KAFKA_SERVER_URL = "kafkaServerURL";
    private static final String KAFKA_SERVER_PORT = "kafkaServerPort";
    private static final String TOPIC = "topic";
    private static final String PARTITION = "partition";

    private String topic;
    private String brokerHost;
    private Integer brokerPort;
    private Integer partition;
    private Integer maxSizeOfFetchMessages = 100;


    public KafkaProperties(RiverSettings riverSettings) {

        if (riverSettings.settings().containsKey("kafka")) {
            Map<String, Object> kafkaSettings = (Map<String, Object>) riverSettings.settings().get("kafka");

            topic = (String) kafkaSettings.get("topic");
            brokerHost = XContentMapValues.nodeStringValue(kafkaSettings.get(KAFKA_SERVER_URL), "localhost");
            brokerPort = XContentMapValues.nodeIntegerValue(kafkaSettings.get(KAFKA_SERVER_PORT), 9092);
            partition = XContentMapValues.nodeIntegerValue(kafkaSettings.get("partition"), 0);

            this.setProperty(TOPIC, topic);
            this.setProperty(KAFKA_SERVER_URL, brokerHost);
            this.setProperty(KAFKA_SERVER_PORT, String.valueOf(brokerPort));
            this.setProperty(PARTITION, String.valueOf(partition));

        } else {
            Properties prop = new Properties();

            try {
                // Load a properties file
                prop.load(new FileInputStream("kafka.properties"));

                // Retrieve the default properties
                brokerHost = prop.getProperty(KAFKA_SERVER_URL);
                brokerPort = Integer.valueOf(prop.getProperty(KAFKA_SERVER_PORT));
                topic = prop.getProperty(TOPIC);
                partition = Integer.valueOf(prop.getProperty(PARTITION));

            } catch (IOException ex) {
                ex.printStackTrace();
            }
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
