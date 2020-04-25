package Benchmark.Debug;

import Benchmark.Config.ConfigFile;

import java.util.ArrayList;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

/**
 * A bodged communication-channel between the ingestion- and query-handlers for the proxy debug option.
 * This lets the injected ingestion-target and the query-target synchronize to alternate their execution
 * through the use of 2 barriers, and enables sharing messages between the threads.
 */
public class PartitionLockstepChannel{
    private final CyclicBarrier insertBarrier = new CyclicBarrier(2);
    private final CyclicBarrier queryBarrier = new CyclicBarrier(2);
    private volatile boolean firstLock = true;
    private ArrayList<QueryDuration> unreadDurations = new ArrayList<>();

    public PartitionLockstepChannel(ConfigFile config){
        assert config.DEBUG_isPartitionLockstepEnabled() : "Why are we instantiating the partition-lockstep channel without that option enabled?";

        if(!(config.isIngestionEnabled() && config.isQueryingEnabled())){
            throw new IllegalStateException("This debug setting needs both ingestion and querying to be enabled.");
        } else if(!(config.getQueriesThreadCount() == 1 && config.getIngestThreadCount() == 1)){
            throw new IllegalStateException("This debug setting is only implemented for the case where we have 1 ingest-thread and 1 query-thread.");
        }
    }

    public synchronized void sendDuration(QueryDuration duration){
        unreadDurations.add(duration);
    }

    public synchronized ArrayList<QueryDuration> readAndReset(){
        ArrayList<QueryDuration> unread = unreadDurations;
        unreadDurations = new ArrayList<>();
        return unread;
    }

    public void awaitInsert(String name){
        try {
            if(firstLock) {
                //System.out.println(name + ": Skipped insert barrier");
                firstLock = false;
            } else {
                //System.out.println(name + ": Waiting on insert barrier");
                insertBarrier.await();
                //System.out.println(name + ": Leaving insert barrier");
            }
        } catch (InterruptedException | BrokenBarrierException e) {
            // NOP
        }
    }

    public void awaitQueries(String name){
        try {
            //System.out.println(name + ": Waiting on query barrier");
            queryBarrier.await();
            //System.out.println(name + ": Leaving query barrier");
        } catch (InterruptedException | BrokenBarrierException e) {
            // NOP
        }
    }

    public void breakBarriers(){
        insertBarrier.reset();
        queryBarrier.reset();
    }

    public static class QueryDuration{
        public final int planningTime;
        public final int executionTime;
        public final int totalTime;
        public final String queryName;
        public final boolean detailed;

        QueryDuration(String queryName, String planningTime, String executionTime){
            this.queryName = queryName;
            this.planningTime  = Integer.parseInt(planningTime.substring(planningTime.indexOf(':')+1, planningTime.indexOf('.')).trim());
            this.executionTime = Integer.parseInt(executionTime.substring(executionTime.indexOf(':')+1, executionTime.indexOf('.')).trim());
            this.totalTime = this.planningTime + this.executionTime;
            this.detailed = true;
        }

        QueryDuration(String queryName, int planningTime, int executionTime){
            this.queryName = queryName;
            this.planningTime  = planningTime;
            this.executionTime = executionTime;
            this.totalTime = this.planningTime + this.executionTime;
            this.detailed = true;
        }

        QueryDuration(String queryName, int totalTime){
            this.queryName = queryName;
            this.planningTime  = -1;
            this.executionTime = -1;
            this.totalTime = totalTime;
            this.detailed = false;
        }
    }
}
