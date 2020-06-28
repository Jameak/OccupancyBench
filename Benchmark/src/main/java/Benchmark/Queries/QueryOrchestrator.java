package Benchmark.Queries;

import Benchmark.Config.ConfigFile;
import Benchmark.DateCommunication;
import Benchmark.Generator.GeneratedData.GeneratedFloor;

import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Supplier;

/**
 * Handles runnables, tasks and threads for querying.
 */
public class QueryOrchestrator {
    private final ConfigFile config;
    private final Supplier<IQueries> querySupplier;
    private final ExecutorService threadPoolQueries;
    private final Future[] queryTasks;
    private final QueryRunnable[] queryRunnables;

    public QueryOrchestrator(ConfigFile config, Supplier<IQueries> querySupplier){
        this.config = config;
        this.querySupplier = querySupplier;
        if(!config.isQueryingEnabled()){
            throw new IllegalStateException("Querying is not enabled. Dont instantiate the query-orchestrator");
        }
        if(config.getQueriesThreadCount() < 1){
            throw new IllegalStateException("Less than 1 query-thread is configured. Must be an invalid config that wasn't caught earlier...");
        }
        threadPoolQueries = Executors.newFixedThreadPool(config.getQueriesThreadCount());
        queryTasks = new Future[config.getQueriesThreadCount()];
        queryRunnables = new QueryRunnable[config.getQueriesThreadCount()];
    }

    public void prepareQuerying(GeneratedFloor[] generatedFloors, Random queryRngSource, DateCommunication dateComm){
        IQueries queryInstance = null;
        if(config.useSharedQueriesInstance()) queryInstance = querySupplier.get();

        for(int i = 0; i < config.getQueriesThreadCount(); i++){
            Random queryRngForThisThread = new Random(queryRngSource.nextInt());
            if(!config.useSharedQueriesInstance()) queryInstance = querySupplier.get();

            queryRunnables[i] = new QueryRunnable(config, queryRngForThisThread, dateComm, generatedFloors, queryInstance, "Query " + i, i);
        }
    }

    public void startQuerying(){
        for(int i = 0; i < queryTasks.length; i++){
            queryTasks[i] = threadPoolQueries.submit(queryRunnables[i]);
        }
    }

    public void waitUntilQuerythreadsFinish() throws ExecutionException, InterruptedException {
        for(Future queryTask : queryTasks){
            queryTask.get();
        }
    }

    public void shutdownQuerying(){
        //@NOTE: This makes it to we need to re-create our threadpool if we want to re-use the orchestrator at a later point.
        //       Not relevant for my use-case so this is fine, but could be improved.
        threadPoolQueries.shutdown();
    }
}
