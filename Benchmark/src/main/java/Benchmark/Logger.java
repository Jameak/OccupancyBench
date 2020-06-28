package Benchmark;

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

/**
 * Facilitates synchronized logging to standard out.
 */
public class Logger {
    private static final Object printLock = new Object();
    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("HH:mm:ss");

    public static void LOG(LogMessage message){
        synchronized (printLock){
            message.doLog();
        }
    }

    public static void LOG(String message){
        synchronized (printLock){
            System.out.println(LocalTime.now().format(formatter) + ": " + message);
        }
    }

    /**
     * Use this to take the print-lock to write several log-messages in a row
     * while avoiding interleaving them with log-messages from other threads.
     */
    public interface LogMessage{
        void doLog();
    }
}
