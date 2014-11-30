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

/**
 * The configuration properties that the client will provide while creating a river in elastic search.
 *
 * @author Mariam Hakobyan
 */
public class RiverConfig {

    /* Kakfa config */
    static final String ZOOKEEPER_CONNECT = "zookeeper.connect";
    private static final String ZOOKEEPER_CONNECTION_TIMEOUT = "zookeeper.connection.timeout.ms";
    private static final String TOPIC = "topic";

    /* Elasticsearch config */
    private static final String INDEX_NAME = "index";
    private static final String MAPPING_TYPE = "type";
    private static final String BULK_SIZE = "bulk.size";
    private static final String CONCURRENT_REQUESTS = "concurrent.requests";
    private static final String KAFKA_MESSAGE_JSON = "kafka.message.json";
    private static final String LOG_KAFKA_MESSAGE = "log.kafka.message";

    /* Default values */
    private static final String DEFAULT_ZOOKEEPER_CONNECT = "localhost";
    private static final int DEFAULT_ZOOKEEPER_CONNECTION_TIMEOUT = 10000;
    private static final String DEFAULT_TOPIC = "elasticsearch-river-kafka";


    private String zookeeperConnect;
    private int zookeeperConnectionTimeout;
    private String topic;
    private String indexName;
    private String typeName;
    private int bulkSize;
    private int concurrentRequests;
    private Boolean kafkaMessageJson;
    private Boolean logKafkaMessage;

    public RiverConfig(RiverName riverName, RiverSettings riverSettings) {

        // Extract kafka related configuration
        if (riverSettings.settings().containsKey("kafka")) {
            Map<String, Object> kafkaSettings = (Map<String, Object>) riverSettings.settings().get("kafka");

            topic = (String) kafkaSettings.get(TOPIC);
            zookeeperConnect = XContentMapValues.nodeStringValue(kafkaSettings.get(ZOOKEEPER_CONNECT), DEFAULT_ZOOKEEPER_CONNECT);
            zookeeperConnectionTimeout = XContentMapValues.nodeIntegerValue(kafkaSettings.get(ZOOKEEPER_CONNECTION_TIMEOUT), DEFAULT_ZOOKEEPER_CONNECTION_TIMEOUT);
        } else {
            zookeeperConnect = DEFAULT_ZOOKEEPER_CONNECT;
            zookeeperConnectionTimeout = DEFAULT_ZOOKEEPER_CONNECTION_TIMEOUT;
            topic = DEFAULT_TOPIC;
        }

        // Extract elasticsearch related configuration
        if (riverSettings.settings().containsKey("index")) {
            Map<String, Object> indexSettings = (Map<String, Object>) riverSettings.settings().get("index");
            indexName = XContentMapValues.nodeStringValue(indexSettings.get(INDEX_NAME), riverName.name());
            typeName = XContentMapValues.nodeStringValue(indexSettings.get(MAPPING_TYPE), "status");
            bulkSize = XContentMapValues.nodeIntegerValue(indexSettings.get(BULK_SIZE), 100);
            concurrentRequests = XContentMapValues.nodeIntegerValue(indexSettings.get(CONCURRENT_REQUESTS), 1);
            kafkaMessageJson = XContentMapValues.nodeBooleanValue(indexSettings.get(KAFKA_MESSAGE_JSON), Boolean.FALSE);
            logKafkaMessage = XContentMapValues.nodeBooleanValue(indexSettings.get(LOG_KAFKA_MESSAGE), Boolean.TRUE);
        } else {
            indexName = riverName.name();
            typeName = "status";
            bulkSize = 100;
            concurrentRequests = 1;
            kafkaMessageJson = false;
            logKafkaMessage = true;
        }
    }

    String getTopic() {
        return topic;
    }

    String getZookeeperConnect() {
        return zookeeperConnect;
    }

    int getZookeeperConnectionTimeout() {
        return zookeeperConnectionTimeout;
    }

    String getIndexName() {
        return indexName;
    }

    String getTypeName() {
        return typeName;
    }

    int getBulkSize() {
        return bulkSize;
    }

    int getConcurrentRequests() {
        return concurrentRequests;
    }

    Boolean getKafkaMessageJson() {
        return kafkaMessageJson;
    }

    Boolean getLogKafkaMessage() {
        return logKafkaMessage;
    }
}
