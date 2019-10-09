package Benchmark;

public class CoarseTimer {
    private long start;
    public void start(){
        start = System.currentTimeMillis();
    }
    public double elapsedMilliseconds(){
        return System.currentTimeMillis() - start;
    }
}
