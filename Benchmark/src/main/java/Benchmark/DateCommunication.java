package Benchmark;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

/**
 * Facilitates synchronized communication between the ingestion- and queries-threads for date/time metadata.
 */
public class DateCommunication {
    private final Object timeLock = new Object();
    private boolean initial_val_set;

    private volatile LocalDateTime newestDateTime;

    public void setInitialDate(LocalDate date, LocalTime time){
        assert !initial_val_set : "DateCommunication::setInitialDate called more than once";
        newestDateTime = date.atTime(time);
        initial_val_set = true;
    }

    public void setNewestTime(LocalDateTime datetime){
        assert initial_val_set : "DateCommunication::setInitialDate wasn't called.";
        synchronized (timeLock){
            if(datetime.isAfter(newestDateTime)) {
                newestDateTime = datetime;
            }
        }
    }

    public LocalDateTime getNewestTime(){
        return newestDateTime;
    }
}
