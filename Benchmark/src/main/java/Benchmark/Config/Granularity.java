package Benchmark.Config;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.concurrent.TimeUnit;

/**
 * Enum specifying the timestamp-granularity that generated data can have.
 */
public enum Granularity{
    NANOSECOND, MICROSECOND, MILLISECOND, SECOND, MINUTE;

    public TimeUnit toTimeUnit(){
        switch (this){
            case NANOSECOND:
                return TimeUnit.NANOSECONDS;
            case MICROSECOND:
                return TimeUnit.MICROSECONDS;
            case MILLISECOND:
                return TimeUnit.MILLISECONDS;
            case SECOND:
                return TimeUnit.SECONDS;
            case MINUTE:
                return TimeUnit.MINUTES;
            default:
                throw new IllegalStateException("Unexpected value: " + this);
        }
    }

    public long getTime(LocalDateTime datetime){
        return getTime(datetime.toInstant(ZoneOffset.ofHours(0)), datetime);
    }

    public long getTime(Instant instant, LocalDateTime datetime){
        assert instant.equals(datetime.toInstant(ZoneOffset.ofHours(0))) :
                "'instant' and 'datetime' must correspond to same point in time. (bad API design but it allows us to cache the 'instant'-instance)";

        switch (this){
            case NANOSECOND:
                return instant.getEpochSecond() * 1_000_000_000 + datetime.getNano();
            case MICROSECOND:
                return (instant.getEpochSecond() * 1_000_000_000 + datetime.getNano()) / 1000;
            case MILLISECOND:
                return instant.toEpochMilli();
            case SECOND:
                return instant.getEpochSecond();
            case MINUTE:
                return instant.getEpochSecond() / 60;
            default:
                throw new IllegalStateException("Unexpected value: " + this);
        }
    }
}