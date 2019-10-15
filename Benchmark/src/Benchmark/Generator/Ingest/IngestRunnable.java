package Benchmark.Generator.Ingest;

import Benchmark.Config.ConfigFile;
import Benchmark.Generator.GeneratedData.AccessPoint;
import Benchmark.Generator.DataGenerator;
import Benchmark.Loader.MapData;
import Benchmark.Generator.Targets.ITarget;
import Benchmark.Generator.Targets.IngestWrapper;
import Benchmark.Logger;

import java.io.IOException;
import java.time.LocalDate;
import java.util.Random;

/**
 * The top-level class for ingestion. Wraps the ingestion-target in a new target that injects an IngestControl-instance
 * whose add-method is called after each generation, to monitor and control ingestion.
 */
public class IngestRunnable implements Runnable {
    private final ConfigFile config;
    private final AccessPoint[] APs;
    private final MapData data;
    private final Random rng;
    private final IngestWrapper targetWrapper;
    private final IngestControl ingestControl;
    private final Logger logger;
    private final String threadName;

    public IngestRunnable(ConfigFile config, AccessPoint[] APs, MapData data, Random rng, ITarget outputTarget, Logger logger, String threadName){
        this.config = config;
        this.APs = APs;
        this.data = data;
        this.rng = rng;
        this.logger = logger;
        this.threadName = threadName;
        this.ingestControl = new IngestControl(config.getIngestSpeed(), config.getIngestReportFrequency(), logger, threadName);
        this.targetWrapper = new IngestWrapper(outputTarget, ingestControl);
    }

    public void stop(){
        targetWrapper.setStop();
    }

    @Override
    public void run() {
        try {
            DataGenerator.Generate(APs, data, config.getIngestStartDate(), LocalDate.MAX, rng, targetWrapper, config);
        } catch (IOException e) {
            logger.log(threadName + ": Ingestion failed.");
            e.printStackTrace();
        }

        ingestControl.printFinalStats();
    }
}
