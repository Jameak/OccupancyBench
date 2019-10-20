package Benchmark;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

/**
 * Facilitates synchronized communication between the ingestion- and queries-threads for date/time metadata.
 */
public class DateCommunication {
    private final Object entryLock = new Object();
    private boolean initial_val_set;

    private LocalDate newestDate;
    private LocalTime newestTime;
    private volatile LocalDateTime time;
    private volatile boolean updated;

    public void setInitialDate(LocalDate date, LocalTime time){
        assert !initial_val_set : "DateCommunication::setInitialDate called more than once";
        newestDate = date;
        newestTime = time;
        initial_val_set = true;
    }

    public void setNewestTime(LocalDate date, LocalTime time){
        assert initial_val_set;
        synchronized (entryLock){
            if(date.isAfter(newestDate)){
                updated = false;
                newestDate = date;
                newestTime = time;
            } else if(date.isEqual(newestDate) && time.isAfter(newestTime)){
                updated = false;
                newestTime = time;
            }
        }
    }

    public LocalDateTime getNewestTime(){
        if(updated){
            return time;
        }

        synchronized (entryLock){
            time = LocalDateTime.of(newestDate, newestTime);
            updated = true;
            return time;
        }
    }
}
