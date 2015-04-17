package org.elasticsearch.river.kafka;

import org.elasticsearch.common.joda.time.DateTime;

/**
 * Resolves the elasticsearch index using time-based configuration settings.
 * Created by kangell on 4/14/2015.
 */
public class TimeBasedIndexNameResolver {
    private String currentIndexName;
    private DateTime nextRollOver;
    private final String baseIndexName;
    private RolloverInterval rolloverInterval;

    public TimeBasedIndexNameResolver(String baseIndexName, RolloverInterval rolloverInterval) {
        this(baseIndexName, rolloverInterval, DateTime.now());
    }

    public TimeBasedIndexNameResolver(String baseIndexName, RolloverInterval rolloverInterval, DateTime initialDate) {
        this.baseIndexName = baseIndexName;
        this.rolloverInterval = rolloverInterval;
        setNextRollOver(initialDate);
        setCurrentIndexName(initialDate);
    }

    public String getIndexName() {
        if (currentIndexName == null) {
            return setCurrentIndexName(DateTime.now());
        }
        if (nextRollOver.isBeforeNow()) {
            return setCurrentIndexName(DateTime.now());
        }
        return currentIndexName;
    }

    private String setCurrentIndexName(DateTime now) {

        switch (rolloverInterval) {

            case NONE:
                currentIndexName = baseIndexName;
                break;
            case WEEK:
                final DateTime weekDate = now.weekOfWeekyear().roundFloorCopy();
                currentIndexName = String.format("%s-%04d.%02d", baseIndexName, weekDate.getYear(), weekDate.getWeekOfWeekyear());
                break;
            case DAY:
                final DateTime dayDate = now.dayOfYear().roundFloorCopy();
                currentIndexName = String.format("%s-%04d.%02d.%02d", baseIndexName, dayDate.getYear(), dayDate.getMonthOfYear(), dayDate.getDayOfMonth());
                break;
            case HOUR:
                final DateTime hourDate = now.hourOfDay().roundFloorCopy();
                currentIndexName = String.format("%s-%04d.%02d.%02d.%02d", baseIndexName, hourDate.getYear(), hourDate.getMonthOfYear(), hourDate.getDayOfMonth(), hourDate.getHourOfDay());
                break;
        }

        return currentIndexName;
    }

    public DateTime getNextRollOver() {
        return nextRollOver;
    }

    public RolloverInterval getRolloverInterval() {
        return rolloverInterval;
    }

    private void setNextRollOver(DateTime now) {
        switch (rolloverInterval){
            case NONE:
                nextRollOver = now.year().roundFloorCopy().plusYears(100);
                break;
            case WEEK:
                nextRollOver = now.dayOfYear().roundFloorCopy().minusDays(now.getDayOfWeek()).plusDays(7);
                break;
            case DAY:
                nextRollOver = now.dayOfYear().roundCeilingCopy();
                break;
            case HOUR:
                nextRollOver = now.hourOfDay().roundCeilingCopy();
                break;
        }
    }
}
