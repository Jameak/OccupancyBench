package Benchmark.Generator.Ingest;

import Benchmark.CSVLogger;
import Benchmark.Config.ConfigFile;
import Benchmark.DateCommunication;
import Benchmark.Generator.GeneratedData.GeneratedAccessPoint;
import Benchmark.Generator.DataGenerator;
import Benchmark.Generator.Targets.ITarget;
import Benchmark.Generator.Targets.MultiTarget;
import Benchmark.SeedLoader.Seeddata.SeedEntries;
import Benchmark.Logger;

import java.io.IOException;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.Random;

/**
 * The top-level class for ingestion. Wraps the ingestion-target in an IngestTarget-instance
 * that monitors ingestion, can add delays, etc.
 */
public class IngestRunnable implements Runnable {
    private final ConfigFile config;
    private final GeneratedAccessPoint[] APs;
    private final SeedEntries data;
    private final Random rng;
    private final ITarget actualTarget;
    private final ITarget combinedTarget;
    private final IngestTarget ingestTarget;
    private final String threadName;
    private final LocalDate endDate;
    private final CSVLogger.IngestLogger csvLogger;

    private boolean done;

    public IngestRunnable(ConfigFile config, GeneratedAccessPoint[] APs, SeedEntries data, Random rng, ITarget outputTarget,
                          DateCommunication dateComm, int threadNumber, LocalDate endDate, boolean doDirectComm){
        this.config = config;
        this.APs = APs;
        this.data = data;
        this.rng = rng;
        this.threadName = "Ingest " + threadNumber;
        this.endDate = endDate;

        if(config.doLoggingToCSV()) csvLogger = CSVLogger.IngestLogger.createInstance(threadName, threadNumber);
        else csvLogger = null;

        this.actualTarget = outputTarget;
        this.ingestTarget = new IngestTarget(config.getIngestSpeed(), config.getIngestReportFrequency(), dateComm,
                threadName, doDirectComm, config.doLoggingToCSV(), csvLogger);
        this.combinedTarget = new MultiTarget(actualTarget, ingestTarget);
    }

    public void stop(){
        // Inject a stop-signal into our IngestTarget so that the Generator stops.
        ingestTarget.setStop();
    }

    @Override
    public void run() {
        if(config.doLoggingToCSV()){
            csvLogger.startTimer();
        }

        try {
            DataGenerator.Generate(APs, data, config.getIngestStartDate(), endDate, rng, combinedTarget, config);
        } catch (IOException | SQLException e) {
            Logger.LOG(threadName + ": Ingestion failed.");
            throw new RuntimeException(e);
        }

        if(actualTarget.shouldStopEarly()){
            Logger.LOG(threadName + ": Error might have occurred during ingestion.");
        }

        ingestTarget.printFinalStats();
        if(config.doLoggingToCSV()){
            csvLogger.setDone();
        }
        done = true;
    }

    public boolean isDone() {
        return done;
    }
}
