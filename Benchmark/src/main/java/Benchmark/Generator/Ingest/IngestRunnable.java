package Benchmark.Generator.Ingest;

import Benchmark.Config.ConfigFile;
import Benchmark.DateCommunication;
import Benchmark.Generator.GeneratedData.AccessPoint;
import Benchmark.Generator.DataGenerator;
import Benchmark.Loader.MapData;
import Benchmark.Generator.Targets.ITarget;
import Benchmark.Generator.Targets.IngestWrapper;
import Benchmark.Logger;

import java.io.IOException;
import java.sql.SQLException;
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
    private final String threadName;
    private final LocalDate endDate;
    private boolean done;

    public IngestRunnable(ConfigFile config, AccessPoint[] APs, MapData data, Random rng, ITarget outputTarget, DateCommunication dateComm, String threadName, LocalDate endDate){
        this.config = config;
        this.APs = APs;
        this.data = data;
        this.rng = rng;
        this.threadName = threadName;
        this.endDate = endDate;
        this.ingestControl = new IngestControl(config.getIngestSpeed(), config.getIngestReportFrequency(), dateComm, threadName);
        this.targetWrapper = new IngestWrapper(outputTarget, ingestControl);
    }

    public void stop(){
        targetWrapper.setStop();
    }

    @Override
    public void run() {
        try {
            DataGenerator.Generate(APs, data, config.getIngestStartDate(), endDate, rng, targetWrapper, config);
        } catch (IOException | SQLException e) {
            Logger.LOG(threadName + ": Ingestion failed.");
            e.printStackTrace();
        }

        if(targetWrapper.didWrappedTargetCauseStop()){
            Logger.LOG(threadName + ": Error might have occurred during ingestion.");
        }

        ingestControl.printFinalStats();
        done = true;
    }

    public boolean isDone() {
        return done;
    }
}
