package Benchmark.Generator.Ingest;

import Benchmark.Config.ConfigFile;
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
    private final Floor[] generatedFloors;
    private final MapData data;
    private final Random rng;
    private final IngestWrapper targetWrapper;
    private final IngestControl ingestControl;
    private final Logger logger;

    public IngestRunnable(ConfigFile config, Floor[] generatedFloors, MapData data, Random rng, ITarget outputTarget, Logger logger){
        this.config = config;
        this.generatedFloors = generatedFloors;
        this.data = data;
        this.rng = rng;
        this.logger = logger;
        this.ingestControl = new IngestControl(config.desiredIngestSpeed(), config.reportFrequency(), logger);
        this.targetWrapper = new IngestWrapper(outputTarget, ingestControl);
    }

    public void stop(){
        targetWrapper.setStop();
    }

    @Override
    public void run() {
        try {
            DataGenerator.Generate(generatedFloors, data, config.ingestStartDate(), LocalDate.MAX, rng, targetWrapper, config);
        } catch (IOException e) {
            logger.log("Ingestion failed.");
            e.printStackTrace();
        }

        ingestControl.printFinalStats();
    }
}
