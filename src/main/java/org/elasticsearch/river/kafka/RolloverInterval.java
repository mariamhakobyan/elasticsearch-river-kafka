package org.elasticsearch.river.kafka;

/**
 * Defines a rollover interval.
 * Created by kangell on 4/14/2015.
 */
public enum RolloverInterval {
    WEEK("week"),
    DAY("day"),
    HOUR("hour");

    private final String rolloverInterval;

    RolloverInterval(String interval) {
        rolloverInterval = interval;
    }

    public String toValue() {
        return rolloverInterval;
    }

    public static RolloverInterval fromValue(String value) {
        if(value == null) throw new IllegalArgumentException();

        for(RolloverInterval values : values()) {
            if(value.equalsIgnoreCase(values.toValue()))
                return values;
        }

        throw new IllegalArgumentException("RolloverInterval with value " + value + " does not exist.");
    }
}
