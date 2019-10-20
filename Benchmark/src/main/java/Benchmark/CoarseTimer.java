package Benchmark;

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
