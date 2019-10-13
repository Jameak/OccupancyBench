package Benchmark;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

/**
 * Facilitates synchronized logging, and communication between the ingestion- and queries-threads for date/time metadata.
 */
public class Logger {
    private final Object entryLock = new Object();
    private final Object printLock = new Object();
    private boolean initial_val_set;

    private LocalDate newestDate;
    private LocalTime newestTime;
    private volatile LocalDateTime time;
    private volatile boolean updated;

    public void setDirectly(LocalDate date, LocalTime time){
        assert !initial_val_set : "Logger::setDirectly called more than once";
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

    public void log(LogMessage message){
        synchronized (printLock){
            message.doLog();
        }
    }

    public void log(String message){
        synchronized (printLock){
            System.out.println(LocalTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss")) + ": " + message);
        }
    }

    public interface LogMessage{
        void doLog();
    }
}
