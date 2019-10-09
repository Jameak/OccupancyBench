package Benchmark.Queries;

import Benchmark.Config.ConfigFile;
import Benchmark.Generator.Ingest.IngestRunnable;
import Benchmark.PreciseTimer;

import java.util.Random;

public class QueryRunnable implements Runnable {
    private final PreciseTimer timer;
    private final ConfigFile config;
    private final IngestRunnable ingest;

    public QueryRunnable(ConfigFile config, Random rng, IngestRunnable ingest){
        this.config = config;
        this.ingest = ingest;
        this.timer = new PreciseTimer();
    }

    @Override
    public void run() {
        timer.start();
        try {
            Thread.sleep(120000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        if(ingest != null) ingest.stop();
    }
}
