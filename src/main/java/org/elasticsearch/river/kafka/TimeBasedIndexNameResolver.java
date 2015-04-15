package org.elasticsearch.river.kafka;

import org.elasticsearch.common.joda.time.DateTime;

/**
 * Resolves the elasticsearch index using time-based configuration settings.
 * Created by kangell on 4/14/2015.
 */
public class TimeBasedIndexNameResolver implements IndexNameResolver {
    private final RiverConfig config;
    private String currentIndexName;
    private DateTime nextRollOver;
    private RolloverInterval rolloverInterval;

    public TimeBasedIndexNameResolver(RiverConfig config) {
        this(config, DateTime.now());
    }

    public TimeBasedIndexNameResolver(RiverConfig config, DateTime initialDate) {
        this.config = config;
        this.rolloverInterval = config.getRolloverInterval();
        setNextRollOver(initialDate);
        setCurrentIndexName(initialDate);
    }

    @Override
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

        switch (rolloverInterval){

            case WEEK:
                final DateTime weekDate = now.weekOfWeekyear().roundFloorCopy();
                currentIndexName = String.format("%s-%04d.%02d", config.getIndexName(), weekDate.getYear(), weekDate.getWeekOfWeekyear());
                break;
            case DAY:
                final DateTime dayDate = now.dayOfYear().roundFloorCopy();
                currentIndexName = String.format("%s-%04d.%02d.%02d", config.getIndexName(), dayDate.getYear(), dayDate.getMonthOfYear(), dayDate.getDayOfMonth());
                break;
            case HOUR:
                final DateTime hourDate = now.hourOfDay().roundFloorCopy();
                currentIndexName = String.format("%s-%04d.%02d.%02d.%02d", config.getIndexName(), hourDate.getYear(), hourDate.getMonthOfYear(), hourDate.getDayOfMonth(), hourDate.getHourOfDay());
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
