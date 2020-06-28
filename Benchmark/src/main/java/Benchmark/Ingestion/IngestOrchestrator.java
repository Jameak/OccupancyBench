package Benchmark.Ingestion;

import Benchmark.Config.ConfigFile;
import Benchmark.Databases.DatabaseTargetFactory;
import Benchmark.DateCommunication;
import Benchmark.Debug.PartitionLockstepChannel;
import Benchmark.Debug.PartitionLockstepIngestionController;
import Benchmark.Generator.GeneratedData.GeneratedAccessPoint;
import Benchmark.Generator.GeneratedData.GeneratedFloor;
import Benchmark.Generator.Targets.ITarget;
import Benchmark.Generator.Targets.MultiTarget;
import Benchmark.SeedLoader.Seeddata.SeedEntries;

import java.io.IOException;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Handles runnables, tasks and threads for ingestion.
 */
public class IngestOrchestrator {
    private final ConfigFile config;
    private final ExecutorService threadPoolIngest;
    private final Future[] ingestTasks;
    private final IngestRunnable[] ingestRunnables;
    private final ITarget[] ingestTargets;

    public IngestOrchestrator(ConfigFile config){
        this.config = config;
        if(!config.isIngestionEnabled()){
            throw new IllegalStateException("Ingestion is not enabled. Dont instantiate the ingestion-orchestrator");
        }
        if(config.getIngestThreadCount() < 1){
            throw new IllegalStateException("Less than 1 ingestion-thread is configured. Must be an invalid config that wasn't caught earlier...");
        }
        threadPoolIngest = Executors.newFixedThreadPool(config.getIngestThreadCount());
        ingestTasks = new Future[config.getIngestThreadCount()];
        ingestRunnables = new IngestRunnable[config.getIngestThreadCount()];
        ingestTargets = new ITarget[config.useSharedIngestInstance() ? 1 : config.getIngestThreadCount()];
    }

    public void startIngestion(){
        for(int i = 0; i < ingestTasks.length; i++){
            ingestTasks[i] = threadPoolIngest.submit(ingestRunnables[i]);
        }
    }

    public void prepareIngestion(GeneratedFloor[] generatedFloors, SeedEntries seedEntries,
                                  DateCommunication dateComm, Random ingestRngSource, boolean doDirectComm,
                                  PartitionLockstepChannel DEBUG_partitionLockstepChannel) throws IOException, SQLException {
        GeneratedAccessPoint[] allAPs = GeneratedFloor.allAPsOnFloors(generatedFloors);

        if(config.useSharedIngestInstance()) {
            ingestTargets[0] = DatabaseTargetFactory.createDatabaseTarget(config.getIngestTarget(), config, config.recreateIngestTarget(), allAPs);
        }

        GeneratedAccessPoint[][] partitionedAPs = evenlyPartitionAPs(allAPs, config.getIngestThreadCount());
        boolean firstCreatedTarget = true;
        //TODO: Might need some functionality to ensure that the ingest-threads are kept similar in speeds.
        //      Otherwise one ingest thread might end up several hours/days in front of the others which then makes
        //      any queries for 'recent' data too easy. Or I could make queries for 'recent' data be the recency of
        //      the slowest thread (currently it follows the fastest one).
        for(int i = 0; i < config.getIngestThreadCount(); i++) {
            Random ingestRngForThread = new Random(ingestRngSource.nextInt());
            ITarget ingestTarget;
            if (config.useSharedIngestInstance()) {
                ingestTarget = ingestTargets[0];
            } else {
                // Only recreate the ingest-target during the first initialization. Avoids churn on the database/target.
                //   Probably doesn't really matter since we create all the instances before ingest begins.
                boolean recreate = false;
                if (firstCreatedTarget) {
                    recreate = config.recreateIngestTarget();
                    firstCreatedTarget = false;
                }
                ingestTarget = DatabaseTargetFactory.createDatabaseTarget(config.getIngestTarget(), config, recreate, allAPs);

                if(config.DEBUG_isPartitionLockstepEnabled()) ingestTarget = new MultiTarget(ingestTarget, new PartitionLockstepIngestionController(config, DEBUG_partitionLockstepChannel));

                ingestTargets[i] = ingestTarget;
            }

            // If ingestion runs alongside querying then ingestion is stopped when we're done querying.
            // If ingestion runs on its own, then we run ingestion until we hit the configured end-date.
            LocalDate ingestEndDate = config.isQueryingEnabled() ? LocalDate.MAX : config.getIngestEndDate();
            ingestRunnables[i] = new IngestRunnable(config, partitionedAPs[i], seedEntries, ingestRngForThread, ingestTarget, dateComm, i, ingestEndDate, doDirectComm);
        }
    }

    public boolean hasAllIngestThreadsFinished(){
        for (IngestRunnable ingestRunnable : ingestRunnables) {
            if (!ingestRunnable.isDone()) {
                return false;
            }
        }
        return true;
    }

    public void shutdownIngestion() throws Exception {
        // Signal to all the threads to stop
        for(IngestRunnable ingestRunnable : ingestRunnables){
            ingestRunnable.stop();
        }

        // Then wait for them to stop.
        for (Future ingestTask : ingestTasks) {
            ingestTask.get();
        }

        // Then we close their connections
        for(ITarget ingestTarget : ingestTargets){
            ingestTarget.close();
        }

        //@NOTE: This makes it to we need to re-create our threadpool if we want to re-use the orchestrator at a later point.
        //       Not relevant for my use-case so this is fine, but could be improved.
        threadPoolIngest.shutdown();
    }

    private GeneratedAccessPoint[][] evenlyPartitionAPs(GeneratedAccessPoint[] allAPs, int partitions){
        List<List<GeneratedAccessPoint>> results = new ArrayList<>(partitions);
        for(int i = 0; i < partitions; i++){
            results.add(new ArrayList<>());
        }
        for(int i = 0; i < allAPs.length; i++){
            results.get(i % partitions).add(allAPs[i]);
        }

        GeneratedAccessPoint[][] out = new GeneratedAccessPoint[partitions][];
        for(int i = 0; i < partitions; i++){
            out[i] = results.get(i).toArray(new GeneratedAccessPoint[0]);
        }

        return out;
    }
}
