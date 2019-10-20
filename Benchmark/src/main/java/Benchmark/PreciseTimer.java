package Benchmark;

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
