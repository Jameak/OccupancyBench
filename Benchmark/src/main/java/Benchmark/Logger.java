package Benchmark;

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

/**
 * Facilitates synchronized logging.
 */
public class Logger {
    private static final Object printLock = new Object();

    public static void LOG(LogMessage message){
        synchronized (printLock){
            message.doLog();
        }
    }

    public static void LOG(String message){
        synchronized (printLock){
            System.out.println(LocalTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss")) + ": " + message);
        }
    }

    public interface LogMessage{
        void doLog();
    }
}
