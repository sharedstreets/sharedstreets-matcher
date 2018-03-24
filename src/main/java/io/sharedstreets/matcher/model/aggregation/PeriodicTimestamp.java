package io.sharedstreets.matcher.model.aggregation;

import io.sharedstreets.matcher.model.Week;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;

public class PeriodicTimestamp {

    public enum CYCLE_PERIOD {

        OneSecond(0),
        FiveSeconds(1),
        TenSeconds(2),
        FifteenSeconds(3),
        ThirtySeconds(4),
        OneMinute(5),
        FiveMinutes(6),
        TenMinutes(7),
        FifteenMinutes(8),
        ThirtyMinutes(9),
        OneHour(10),
        OneDay(11),
        OneWeek(12),
        OneMonth(13),
        OneYear(14);

        private final int value;

        CYCLE_PERIOD(final int newValue) {
            value = newValue;
        }

        public int getValue() {
            return value;
        }

    }

    public static  ZoneId UTC_TIMEZONE = ZoneId.of("UTC");

    CYCLE_PERIOD cyclePeriod = CYCLE_PERIOD.OneHour; // only OneHour implemented

    public ZoneId timeZone;
    public Week week;
    public int period;
    public long timestamp;

    public static PeriodicTimestamp utcPeriodTimestamp(long timestamp) throws Exception {
        return new PeriodicTimestamp(timestamp, UTC_TIMEZONE);
    }

    public PeriodicTimestamp(long time, ZoneId timeZone) throws Exception {

        this.timeZone = timeZone;

        this.timestamp = time;

        // check and convert to millisecond -- comparing with 2000-05-01 (date of GPS selective availability change)
        if(this.timestamp  < 957139200l)
            this.timestamp = this.timestamp  * 1000;

        Instant currentTime = Instant.ofEpochMilli(this.timestamp);
        ZonedDateTime zonedDateTime = ZonedDateTime.ofInstant(currentTime, this.timeZone);
        int dayOfWeek = zonedDateTime.get(ChronoField.DAY_OF_WEEK) - 1;
        int hourOfDay = zonedDateTime.get(ChronoField.HOUR_OF_DAY);

        // calc beginning of week
        ZonedDateTime mondayDateTime = zonedDateTime.minus(dayOfWeek, ChronoUnit.DAYS);

        week = new Week();
        week.year = mondayDateTime.getYear();
        week.month = mondayDateTime.getMonthValue();
        week.day = mondayDateTime.getDayOfMonth();

        period = (dayOfWeek * 24) + hourOfDay;
    }

}
