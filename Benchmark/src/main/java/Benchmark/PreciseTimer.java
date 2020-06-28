package Benchmark;

/**
 * Timer with nanosecond granularity.
 * See {@code System.nanoTime()} for time limitations.
 */
public class PreciseTimer {
    private long start;

    public void start(){
        start = System.nanoTime();
    }

    public long elapsedNanoseconds(){
        return System.nanoTime() - start;
    }

    public double elapsedSeconds(){
        return (System.nanoTime() - start) / 1e9;
    }
}
