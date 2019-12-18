package com.cwoongc.kafka.kafkacommittest.util;

import java.time.*;
import java.util.TimeZone;

public final class TimeUtils {
    public static final ZoneId UTC_ZONE_ID = ZoneId.of("UTC");
    public static final TimeZone DEFAULT_TIMEZONE;
    public static final ZoneId DEFAULT_ZONE_ID;
    public static final ZoneOffset DEFAULT_ZONE_OFFSET;
    public static final LocalDateTime LAST_TIMESTAMP = LocalDateTime.of(2038, Month.JANUARY, 1, 0, 0, 0);

    public TimeUtils() {
    }

    public static LocalDateTime convertTimezoneTo(LocalDateTime dateTime, ZoneId zoneIdTo) {
        return convertTimezoneFromTo(dateTime, ZoneId.systemDefault(), zoneIdTo);
    }

    public static LocalDateTime convertTimezoneFrom(LocalDateTime dateTime, ZoneId zoneIdFrom) {
        return convertTimezoneFromTo(dateTime, zoneIdFrom, ZoneId.systemDefault());
    }

    public static LocalDateTime convertTimezoneFromTo(LocalDateTime dateTime, ZoneId zoneIdFrom, ZoneId zoneIdTo) {
        ZonedDateTime zonedDateTime = ZonedDateTime.of(dateTime, zoneIdFrom);
        zonedDateTime = zonedDateTime.withZoneSameInstant(zoneIdTo);
        return zonedDateTime.toLocalDateTime();
    }

    public static long getCurrentEpochTime() {
        return getEpochTime(LocalDateTime.now());
    }

    public static long getEpochTime(LocalDateTime time) {
        return getEpochTime(time, ZoneId.systemDefault());
    }

    public static long getEpochTime(LocalDateTime time, ZoneId zoneId) {
        return time.atZone(zoneId).toInstant().toEpochMilli();
    }

    public static LocalDateTime fromEpochTime(long epochTime) {
        return fromEpochTime(epochTime, ZoneId.systemDefault());
    }

    public static LocalDateTime fromEpochTime(Long epochTime, LocalDateTime defaultValue) {
        return epochTime != null ? fromEpochTime(epochTime, ZoneId.systemDefault()) : defaultValue;
    }

    public static LocalDateTime fromEpochTime(long epochTime, ZoneId zoneId) {
        return LocalDateTime.ofInstant(Instant.ofEpochMilli(epochTime), zoneId);
    }

    public static boolean isBeforeThan(Long startTime, Long endTime) {
        return startTime == null || endTime == null || startTime < endTime;
    }

    static {
        DEFAULT_TIMEZONE = TimeZone.getTimeZone(UTC_ZONE_ID);
        DEFAULT_ZONE_ID = DEFAULT_TIMEZONE.toZoneId();
        DEFAULT_ZONE_OFFSET = ZoneOffset.ofHours(0);
    }
}
