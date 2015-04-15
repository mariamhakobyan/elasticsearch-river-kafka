package org.elasticsearch.river.kafka.test;

import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.river.kafka.RiverConfig;
import org.elasticsearch.river.kafka.RolloverInterval;
import org.elasticsearch.river.kafka.TimeBasedIndexNameResolver;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.*;

/**
 * Created by kangell on 4/14/2015.
 */
public class TimeBasedIndexNameResolverTest {

    @Test
    public void testRolloverDateDay() {
        final Map<String, Object> settings = RiverConfigTest.getSettings(null, null);
        ((Map<String, Object>)settings.get("index")).put(RiverConfig.ROLLOVER_INTERVAL, RolloverInterval.DAY.toValue());

        final TimeBasedIndexNameResolver testTarget = new TimeBasedIndexNameResolver(RiverConfigTest.getRiverConfig(settings));
        final DateTime nextRollOver = testTarget.getNextRollOver();
        final DateTime now = DateTime.now();
        final DateTime expectedDateTime = new DateTime(now.getYear(), now.getMonthOfYear(), now.getDayOfMonth(), 0, 0).plusDays(1);
        assertEquals(RolloverInterval.DAY, testTarget.getRolloverInterval());
        assertEquals(expectedDateTime, nextRollOver);
    }

    @Test
    public void testRolloverDateWeek() {
        final Map<String, Object> settings = RiverConfigTest.getSettings(null, null);
        ((Map<String, Object>)settings.get("index")).put(RiverConfig.ROLLOVER_INTERVAL, RolloverInterval.WEEK.toValue());

        final TimeBasedIndexNameResolver testTarget = new TimeBasedIndexNameResolver(RiverConfigTest.getRiverConfig(settings));
        final DateTime nextRollOver = testTarget.getNextRollOver();
        final DateTime now = DateTime.now();
        final DateTime expectedDateTime = new DateTime(now.getYear(), now.getMonthOfYear(), now.getDayOfMonth()-now.getDayOfWeek(), 0, 0).plusDays(7);
        assertEquals(RolloverInterval.WEEK, testTarget.getRolloverInterval());
        assertEquals(expectedDateTime, nextRollOver);
    }

    @Test
    public void testRolloverIndexNameDay() {
        final Map<String, Object> settings = RiverConfigTest.getSettings(null, "test-index");
        ((Map<String, Object>)settings.get("index")).put(RiverConfig.ROLLOVER_INTERVAL, RolloverInterval.DAY.toValue());

        final TimeBasedIndexNameResolver testTarget = new TimeBasedIndexNameResolver(RiverConfigTest.getRiverConfig(settings));
        final String indexName = testTarget.getIndexName();
        final DateTime now = DateTime.now();
        final String expectedIndexName = String.format("test-index-%04d.%02d.%02d", now.getYear(), now.getMonthOfYear(), now.getDayOfMonth());
        assertEquals("Index names do not match.", expectedIndexName, indexName);
    }

    @Test
    public void testRolloverIndexNameWeek() {
        final Map<String, Object> settings = RiverConfigTest.getSettings(null, "test-index");
        ((Map<String, Object>)settings.get("index")).put(RiverConfig.ROLLOVER_INTERVAL, RolloverInterval.WEEK.toValue());

        final TimeBasedIndexNameResolver testTarget = new TimeBasedIndexNameResolver(RiverConfigTest.getRiverConfig(settings));
        final String indexName = testTarget.getIndexName();
        final DateTime now = DateTime.now();
        final String expectedIndexName = String.format("test-index-%04d.%02d", now.getYear(), now.getWeekOfWeekyear());
        assertEquals("Index names do not match.", expectedIndexName, indexName);
    }

    @Test
    public void testRolloverIndexNameHour() {
        final Map<String, Object> settings = RiverConfigTest.getSettings(null, "test-index");
        ((Map<String, Object>)settings.get("index")).put(RiverConfig.ROLLOVER_INTERVAL, RolloverInterval.HOUR.toValue());

        final TimeBasedIndexNameResolver testTarget = new TimeBasedIndexNameResolver(RiverConfigTest.getRiverConfig(settings));
        final String indexName = testTarget.getIndexName();
        final DateTime now = DateTime.now();
        final String expectedIndexName = String.format("test-index-%04d.%02d.%02d.%02d", now.getYear(), now.getMonthOfYear(), now.getDayOfMonth(), now.getHourOfDay());
        assertEquals("Index names do not match.", expectedIndexName, indexName);
    }

    @Test
    public void testRolloverOfIndexName() {
        final Map<String, Object> settings = RiverConfigTest.getSettings(null, "test-index");
        ((Map<String, Object>)settings.get("index")).put(RiverConfig.ROLLOVER_INTERVAL, RolloverInterval.HOUR.toValue());

        // Create with an old date so it rolls over immediately.
        final TimeBasedIndexNameResolver testTarget = new TimeBasedIndexNameResolver(RiverConfigTest.getRiverConfig(settings), DateTime.now().minusDays(1));
        final String indexName = testTarget.getIndexName();
        final DateTime now = DateTime.now();
        final String expectedIndexName = String.format("test-index-%04d.%02d.%02d.%02d", now.getYear(), now.getMonthOfYear(), now.getDayOfMonth(), now.getHourOfDay());
        assertEquals("Index names do not match.", expectedIndexName, indexName);
    }
}