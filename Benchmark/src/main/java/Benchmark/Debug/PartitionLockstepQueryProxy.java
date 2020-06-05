package Benchmark.Debug;

import Benchmark.Config.ConfigFile;
import Benchmark.Generator.GeneratedData.GeneratedAccessPoint;
import Benchmark.Generator.GeneratedData.GeneratedFloor;
import Benchmark.PreciseTimer;
import Benchmark.Queries.IQueries;
import Benchmark.Queries.Results.*;

import java.io.IOException;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Random;

/**
 * Proxies a given IQueries-implementation, keeping track of the time spent in the query while
 * synchronizing with the lockstep-injection-controller.
 */
public class PartitionLockstepQueryProxy implements IQueries {
    private final IQueries proxiedImpl;
    private final PartitionLockstepChannel channel;
    private final PreciseTimer timer = new PreciseTimer();

    private int queriesSinceLastStop = 0;

    public PartitionLockstepQueryProxy(IQueries proxiedImpl, PartitionLockstepChannel channel){
        this.proxiedImpl = proxiedImpl;
        this.channel = channel;
    }

    private void writeAndAwait(PartitionLockstepChannel.QueryDuration duration){
        channel.sendDuration(duration);
        queriesSinceLastStop++;
        if(queriesSinceLastStop == PartitionLockstepIngestionController.QUERIES_PER_STOPPING_POINT){
            queriesSinceLastStop = 0;
            channel.awaitQueries("QUERY");
            channel.awaitInsert("QUERY");
        }
    }

    @Override
    public void prepare(ConfigFile config, GeneratedFloor[] generatedFloors, Random rng) throws Exception {
        proxiedImpl.prepare(config, generatedFloors, rng);
    }

    @Override
    public void done() throws IOException, SQLException {
        proxiedImpl.done();
    }

    @Override
    public LocalDateTime getNewestTimestamp(LocalDateTime previousNewestTime) throws IOException, SQLException {
        return proxiedImpl.getNewestTimestamp(previousNewestTime);
    }

    @Override
    public List<Total> computeTotalClients(LocalDateTime start, LocalDateTime end) throws IOException, SQLException {
        timer.start();
        List<Total> result =  proxiedImpl.computeTotalClients(start, end);
        int time = (int) Math.round(timer.elapsedSeconds() * 1000);
        writeAndAwait(new PartitionLockstepChannel.QueryDuration("Total Clients", time));
        return result;
    }

    @Override
    public List<FloorTotal> computeFloorTotal(LocalDateTime start, LocalDateTime end) throws IOException, SQLException {
        timer.start();
        List<FloorTotal> result = proxiedImpl.computeFloorTotal(start, end);
        int time = (int) Math.round(timer.elapsedSeconds() * 1000);
        writeAndAwait(new PartitionLockstepChannel.QueryDuration("Floor Total", time));
        return result;
    }

    @Override
    public List<MaxForAP> maxPerDayForAP(LocalDateTime start, LocalDateTime end, GeneratedAccessPoint AP) throws IOException, SQLException {
        timer.start();
        List<MaxForAP> result = proxiedImpl.maxPerDayForAP(start, end, AP);
        int time = (int) Math.round(timer.elapsedSeconds() * 1000);
        writeAndAwait(new PartitionLockstepChannel.QueryDuration("Max for AP", time));
        return result;
    }

    @Override
    public List<AvgOccupancy> computeAvgOccupancy(LocalDateTime start, LocalDateTime end, int windowSizeInMin) throws IOException, SQLException {
        timer.start();
        List<AvgOccupancy> result = proxiedImpl.computeAvgOccupancy(start, end, windowSizeInMin);
        int time = (int) Math.round(timer.elapsedSeconds() * 1000);
        writeAndAwait(new PartitionLockstepChannel.QueryDuration("Avg Occupancy", time));
        return result;
    }

    @Override
    public List<KMeans> computeKMeans(LocalDateTime start, LocalDateTime end, int numClusters, int numIterations) throws IOException, SQLException {
        timer.start();
        List<KMeans> result = proxiedImpl.computeKMeans(start, end, numClusters, numIterations);
        int time = (int) Math.round(timer.elapsedSeconds() * 1000);
        writeAndAwait(new PartitionLockstepChannel.QueryDuration("K-Means", time));
        return result;
    }
}
