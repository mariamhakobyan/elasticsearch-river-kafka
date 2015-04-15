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
    final static String INDEX = "test-index";

    @Test
    public void testRolloverDateDay() {
        final TimeBasedIndexNameResolver testTarget = new TimeBasedIndexNameResolver(INDEX, RolloverInterval.DAY);
        final DateTime nextRollOver = testTarget.getNextRollOver();
        final DateTime now = DateTime.now();
        final DateTime expectedDateTime = new DateTime(now.getYear(), now.getMonthOfYear(), now.getDayOfMonth(), 0, 0).plusDays(1);
        assertEquals(RolloverInterval.DAY, testTarget.getRolloverInterval());
        assertEquals(expectedDateTime, nextRollOver);
    }

    @Test
    public void testRolloverDateWeek() {
        final TimeBasedIndexNameResolver testTarget = new TimeBasedIndexNameResolver(INDEX, RolloverInterval.WEEK);
        final DateTime nextRollOver = testTarget.getNextRollOver();
        final DateTime now = DateTime.now();
        final DateTime expectedDateTime = new DateTime(now.getYear(), now.getMonthOfYear(), now.getDayOfMonth()-now.getDayOfWeek(), 0, 0).plusDays(7);
        assertEquals(RolloverInterval.WEEK, testTarget.getRolloverInterval());
        assertEquals(expectedDateTime, nextRollOver);
    }

    @Test
    public void testRolloverIndexNameDay() {
        final TimeBasedIndexNameResolver testTarget = new TimeBasedIndexNameResolver(INDEX, RolloverInterval.DAY);
        final String indexName = testTarget.getIndexName();
        final DateTime now = DateTime.now();
        final String expectedIndexName = String.format("test-index-%04d.%02d.%02d", now.getYear(), now.getMonthOfYear(), now.getDayOfMonth());
        assertEquals("Index names do not match.", expectedIndexName, indexName);
    }

    @Test
    public void testRolloverIndexNameWeek() {
        final TimeBasedIndexNameResolver testTarget = new TimeBasedIndexNameResolver(INDEX, RolloverInterval.WEEK);
        final String indexName = testTarget.getIndexName();
        final DateTime now = DateTime.now();
        final String expectedIndexName = String.format("test-index-%04d.%02d", now.getYear(), now.getWeekOfWeekyear());
        assertEquals("Index names do not match.", expectedIndexName, indexName);
    }

    @Test
    public void testRolloverIndexNameHour() {
        final TimeBasedIndexNameResolver testTarget = new TimeBasedIndexNameResolver(INDEX, RolloverInterval.HOUR);
        final String indexName = testTarget.getIndexName();
        final DateTime now = DateTime.now();
        final String expectedIndexName = String.format("test-index-%04d.%02d.%02d.%02d", now.getYear(), now.getMonthOfYear(), now.getDayOfMonth(), now.getHourOfDay());
        assertEquals("Index names do not match.", expectedIndexName, indexName);
    }

    @Test
    public void testRolloverOfIndexName() {
        // Create with an old date so it rolls over immediately.
        final TimeBasedIndexNameResolver testTarget = new TimeBasedIndexNameResolver(INDEX, RolloverInterval.HOUR, DateTime.now().minusDays(1));
        final String indexName = testTarget.getIndexName();
        final DateTime now = DateTime.now();
        final String expectedIndexName = String.format("test-index-%04d.%02d.%02d.%02d", now.getYear(), now.getMonthOfYear(), now.getDayOfMonth(), now.getHourOfDay());
        assertEquals("Index names do not match.", expectedIndexName, indexName);
    }
}