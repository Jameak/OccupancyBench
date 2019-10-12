package Benchmark.Generator.Ingest;

import Benchmark.Config.ConfigFile;
import Benchmark.Generator.AccessPoint;
import Benchmark.Generator.DataGenerator;
import Benchmark.Generator.Floor;
import Benchmark.Generator.MapData;
import Benchmark.Generator.Targets.ITarget;
import Benchmark.Generator.Targets.IngestWrapper;
import Benchmark.Logger;

import java.io.IOException;
import java.time.LocalDate;
import java.util.Random;

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
        this.ingestControl = new IngestControl(config.desiredIngestSpeed(), config.reportFrequency(), logger, threadName);
        this.targetWrapper = new IngestWrapper(outputTarget, ingestControl);
    }

    public void stop(){
        targetWrapper.setStop();
    }

    @Override
    public void run() {
        try {
            DataGenerator.Generate(APs, data, config.ingestStartDate(), LocalDate.MAX, rng, targetWrapper, config);
        } catch (IOException e) {
            logger.log(threadName + ": Ingestion failed.");
            e.printStackTrace();
        }

        ingestControl.printFinalStats();
    }
}
