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
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;

import java.util.Map;
import java.util.Properties;

/**
 * The properties that the client will provide while creating a river in elastic search.
 *
 * @author Mariam Hakobyan
 */
public class RiverProperties extends Properties {

    /* Kakfa properties */
    static final String ZOOKEEPER_CONNECT = "zookeeper.connect";
    private static final String ZOOKEEPER_CONNECTION_TIMEOUT = "zookeeper.connection.timeout.ms";
    private static final String TOPIC = "topic";

    private static final String DEFAULT_ZOOKEEPER_CONNECT = "localhost";
    private static final Integer DEFAULT_ZOOKEEPER_CONNECTION_TIMEOUT = 10000;
    private static final String DEFAULT_TOPIC = "default-topic";


    private String topic;
    private String zookeeperConnect;
    private Integer zookeeperConnectionTimeout;

    /* Elasticsearch properties */
    private static final String INDEX_NAME = "index";
    private static final String MAPPING_TYPE= "type";
    private static final String BULK_SIZE= "bulk_size";


    private String indexName;
    private String typeName;
    private Integer bulkSize;


    public RiverProperties(RiverName riverName, RiverSettings riverSettings) {

        // Extract kafka related properties
        if (riverSettings.settings().containsKey("kafka")) {
            Map<String, Object> kafkaSettings = (Map<String, Object>) riverSettings.settings().get("kafka");

            topic = (String) kafkaSettings.get("topic");
            zookeeperConnect = XContentMapValues.nodeStringValue(kafkaSettings.get(ZOOKEEPER_CONNECT), DEFAULT_ZOOKEEPER_CONNECT);
            zookeeperConnectionTimeout = XContentMapValues.nodeIntegerValue(kafkaSettings.get(ZOOKEEPER_CONNECTION_TIMEOUT), DEFAULT_ZOOKEEPER_CONNECTION_TIMEOUT);
        } else {
            // Use the default properties
            zookeeperConnect = DEFAULT_ZOOKEEPER_CONNECT;
            zookeeperConnectionTimeout = DEFAULT_ZOOKEEPER_CONNECTION_TIMEOUT;
            topic = DEFAULT_TOPIC;
        }

        // Extract elasticsearch related properties
        if (riverSettings.settings().containsKey("index")) {
            Map<String, Object> indexSettings = (Map<String, Object>) riverSettings.settings().get("index");
            indexName = XContentMapValues.nodeStringValue(indexSettings.get("index"), riverName.name());
            typeName = XContentMapValues.nodeStringValue(indexSettings.get("type"), "status");
            bulkSize = XContentMapValues.nodeIntegerValue(indexSettings.get("bulk_size"), 100);
        } else {
            indexName = riverName.name();
            typeName = "status";
            bulkSize = 100;
        }

        this.setProperty(TOPIC, topic);
        this.setProperty(ZOOKEEPER_CONNECT, zookeeperConnect);
        this.setProperty(ZOOKEEPER_CONNECTION_TIMEOUT, String.valueOf(zookeeperConnectionTimeout));
        this.setProperty(INDEX_NAME, indexName);
        this.setProperty(MAPPING_TYPE, typeName);
        this.setProperty(BULK_SIZE, String.valueOf(bulkSize));
    }

    String getTopic() {
        return topic;
    }

    String getZookeeperConnect() {
        return zookeeperConnect;
    }

    Integer getZookeeperConnectionTimeout() {
        return zookeeperConnectionTimeout;
    }

    String getIndexName() {
        return indexName;
    }

    String getTypeName() {
        return typeName;
    }

    Integer getBulkSize() {
        return bulkSize;
    }
}
