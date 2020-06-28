package Benchmark;

/**
 * Timer with millisecond granularity.
 * See {@code System.currentTimeMillis()} for granularity limitations.
 */
public class CoarseTimer {
    private long start;

    public void start(){
        start = System.currentTimeMillis();
    }

    public double elapsedMilliseconds(){
        return System.currentTimeMillis() - start;
    }

    public double elapsedSeconds(){
        return (System.currentTimeMillis() - start) / 1e3;
    }
}
