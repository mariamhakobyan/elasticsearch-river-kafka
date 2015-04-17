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
    public static final String ZOOKEEPER_CONNECT = "zookeeper.connect";
    public static final String ZOOKEEPER_CONNECTION_TIMEOUT = "zookeeper.connection.timeout.ms";
    private static final String TOPIC = "topic";
    private static final String MESSAGE_TYPE = "message.type";
    public static final String CONSUMER_GROUP_ID = "group.id";

    /* Elasticsearch config */
    private static final String INDEX_NAME = "index";
    private static final String MAPPING_TYPE = "type";
    private static final String BULK_SIZE = "bulk.size";
    private static final String CONCURRENT_REQUESTS = "concurrent.requests";
    private static final String ACTION_TYPE = "action.type";
    public static final String ROLLOVER_INTERVAL = "rollover.interval";

    public final static String DEFAULT_GROUP_ID = "elasticsearch-kafka-river";

    private String zookeeperConnect;
    private int zookeeperConnectionTimeout;
    private String topic;
    private MessageType messageType;
    private String typeName;
    private int bulkSize;
    private int concurrentRequests;
    private ActionType actionType;
    private final String consumerGroup;
    private final RolloverInterval rolloverInterval;
    private final TimeBasedIndexNameResolver indexNameResolver;

    public RiverConfig(RiverName riverName, RiverSettings riverSettings) {
        String indexName;

        // Extract kafka related configuration
        if (riverSettings.settings().containsKey("kafka")) {
            Map<String, Object> kafkaSettings = (Map<String, Object>) riverSettings.settings().get("kafka");

            topic = (String) kafkaSettings.get(TOPIC);
            consumerGroup = XContentMapValues.nodeStringValue(kafkaSettings.get(CONSUMER_GROUP_ID), DEFAULT_GROUP_ID);
            zookeeperConnect = XContentMapValues.nodeStringValue(kafkaSettings.get(ZOOKEEPER_CONNECT), "localhost");
            zookeeperConnectionTimeout = XContentMapValues.nodeIntegerValue(kafkaSettings.get(ZOOKEEPER_CONNECTION_TIMEOUT), 10000);
            messageType = MessageType.fromValue(XContentMapValues.nodeStringValue(kafkaSettings.get(MESSAGE_TYPE),
                    MessageType.JSON.toValue()));
        } else {
            zookeeperConnect = "localhost";
            zookeeperConnectionTimeout = 10000;
            topic = "elasticsearch-river-kafka";
            messageType = MessageType.JSON;
            consumerGroup = DEFAULT_GROUP_ID;
        }

        // Extract ElasticSearch related configuration
        if (riverSettings.settings().containsKey("index")) {
            Map<String, Object> indexSettings = (Map<String, Object>) riverSettings.settings().get("index");
            indexName = XContentMapValues.nodeStringValue(indexSettings.get(INDEX_NAME), riverName.name());
            String rolloverValue = XContentMapValues.nodeStringValue(indexSettings.get(ROLLOVER_INTERVAL), RolloverInterval.NONE.toValue());
            rolloverInterval = RolloverInterval.fromValue(rolloverValue);
            typeName = XContentMapValues.nodeStringValue(indexSettings.get(MAPPING_TYPE), "status");
            bulkSize = XContentMapValues.nodeIntegerValue(indexSettings.get(BULK_SIZE), 100);
            concurrentRequests = XContentMapValues.nodeIntegerValue(indexSettings.get(CONCURRENT_REQUESTS), 1);
            actionType = ActionType.fromValue(XContentMapValues.nodeStringValue(indexSettings.get(ACTION_TYPE),
                    ActionType.INDEX.toValue()));
        } else {
            indexName = riverName.name();
            typeName = "status";
            bulkSize = 100;
            concurrentRequests = 1;
            actionType = ActionType.INDEX;
            rolloverInterval = RolloverInterval.NONE;
        }

        indexNameResolver = new TimeBasedIndexNameResolver(indexName, rolloverInterval);
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public RolloverInterval getRolloverInterval() {
        return rolloverInterval;
    }

    public enum ActionType {

        INDEX("index"),
        DELETE("delete"),
        RAW_EXECUTE("raw.execute");

        private String actionType;

        private ActionType(String actionType) {
            this.actionType = actionType;
        }

        public String toValue() {
            return actionType;
        }

        public static ActionType fromValue(String value) {
            if(value == null) throw new IllegalArgumentException();

            for(ActionType values : values()) {
                if(value.equalsIgnoreCase(values.toValue()))
                    return values;
            }

            throw new IllegalArgumentException("ActionType with value " + value + " does not exist.");
        }
    }

    public enum MessageType {
        STRING("string"),
        JSON("json");

        private String messageType;

        private MessageType(String messageType) {
            this.messageType = messageType;
        }

        public String toValue() {
            return messageType;
        }

        public static MessageType fromValue(String value) {
            if(value == null) throw new IllegalArgumentException();

            for(MessageType values : values()) {
                if(value.equalsIgnoreCase(values.toValue()))
                    return values;
            }

            throw new IllegalArgumentException("MessageType with value " + value + " does not exist.");
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

    MessageType getMessageType() {
        return messageType;
    }

    String getIndexName() {
        return indexNameResolver.getIndexName();
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

    ActionType getActionType() {
        return actionType;
    }
}
