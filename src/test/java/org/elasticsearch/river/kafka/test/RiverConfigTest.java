package org.elasticsearch.river.kafka.test;

import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;
import org.elasticsearch.river.kafka.RiverConfig;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;

/**
 * Created by kangell on 4/13/2015.
 */
public class RiverConfigTest {

    @Test
    public void testGetConsumerGroup() throws Exception {

        Map<String, Object> settings = getSettings("myGroup");

        final RiverSettings riverSettings = new RiverSettings(null, settings);
        final RiverConfig riverConfig = new RiverConfig(new RiverName("kafka", "kafka-river"), riverSettings);
        Assert.assertEquals("groupId should be custom group", "myGroup", riverConfig.getConsumerGroup());
    }

    @Test
    public void testGetConsumerGroupIsDefault() throws Exception {

        Map<String, Object> settings = getSettings(null);

        final RiverSettings riverSettings = new RiverSettings(null, settings);
        final RiverConfig riverConfig = new RiverConfig(new RiverName("kafka", "kafka-river"), riverSettings);
        Assert.assertEquals("groupId should be default", RiverConfig.DEFAULT_GROUP_ID, riverConfig.getConsumerGroup());
    }

    private Map<String, Object> getSettings(String consumerGroup) {
        Map<String, Object> settings = new HashMap<String, Object>();
        Map<String, Object> kafkaSettings = new HashMap<String, Object>();
        Map<String, Object> indexSettings = new HashMap<String, Object>();

        kafkaSettings.put("zookeeper.connect", "myhost:2081");
        kafkaSettings.put("zookeeper.connection.timeout.ms", 100);
        kafkaSettings.put("topic", "testTopic");
        kafkaSettings.put("message.type", "json");

        if (consumerGroup != null) {
            kafkaSettings.put(RiverConfig.CONSUMER_GROUP_ID, consumerGroup);
        }

        settings.put("kafka", kafkaSettings);

        indexSettings.put("index", "kafka-index");
        indexSettings.put("type", "logs");
        indexSettings.put("bulk.size", 100);
        indexSettings.put("concurrent.requests", 1);
        indexSettings.put("action.type", "index");

        settings.put("index", indexSettings);
        return settings;
    }
}