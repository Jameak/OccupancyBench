package Benchmark;

import java.time.LocalTime;

public class Logger {
    public synchronized void log(String message){
            System.out.println(LocalTime.now().toString() + ": " + message);
    }
}
