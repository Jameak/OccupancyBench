package Benchmark.Queries;

import Benchmark.Config.ConfigFile;
import Benchmark.Generator.GeneratedData.GeneratedAccessPoint;
import Benchmark.Generator.GeneratedData.GeneratedFloor;
import Benchmark.Queries.Results.*;

import java.io.IOException;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Random;

/**
 * A shared interface for all Query-implementations.
 */
public interface IQueries {
    /**
     * This method is called once by the framework, before any queries are executed.
     * Use it to perform whatever pre-computations are possible.
     */
    void prepare(ConfigFile config, GeneratedFloor[] generatedFloors, Random rng) throws Exception;

    /**
     * This method is called once by the framework, after querying is finished.
     * Use it to cleanup resources.
     */
    void done() throws IOException, SQLException;

    /**
     * Query the database for the newest available timestamp and return it.
     * This allows the database to make decisions regarding staleness of data by returning
     * a timestamp that is older than our newest inserted row.
     * @param previousNewestTime The timestamp returned from the last call to this method.
     */
    LocalDateTime getNewestTimestamp(LocalDateTime previousNewestTime) throws IOException, SQLException;

    /**
     * This query computes the total number of clients connected per day within
     * the specified range.
     */
    List<Total> computeTotalClients(LocalDateTime start, LocalDateTime end) throws IOException, SQLException;

    /**
     * This query computes the total number of clients connected per day within
     * a specified range, on a per-floor basis.
     */
    List<FloorTotal> computeFloorTotal(LocalDateTime start, LocalDateTime end) throws IOException, SQLException;

    /**
     * This query computes the max number of clients connected to the specified
     * access point point per day within a specified range.
     */
    List<MaxForAP> maxPerDayForAP(LocalDateTime start, LocalDateTime end, GeneratedAccessPoint AP) throws IOException, SQLException;

    /**
     * This query computes the following:
     * 1) the current number of clients connected to every access point in the
     *    newest entry made available by the database,
     * 2) the average number of clients connected to every access point over the
     *    previous 'x' minutes (specified by the window size), for some number of days,
     * 3) the average number of clients connected to every access point over the
     *    next 'x' minutes (specified by the window size), for some number of previous days.
     *
     * The number of days to compute this for, is specified by the range represented by 'start' and 'end'.
     */
    List<AvgOccupancy> computeAvgOccupancy(LocalDateTime start, LocalDateTime end, int windowSizeInMin) throws IOException, SQLException;

    /**
     * An implementation of the K-Means algorithm.
     *
     * The existing implementations have implemented a modified version of K-Means that has a more
     * interesting data access-pattern than the typical "scan everything, then do computations locally"
     * access-pattern. The existing implementations grab data for a single access point, do computations
     * on that, and then grab data for the next access point.
     * The existing implementations run the following number of queries against the database:
     *     (number of access points) times (number of K-Means iterations)
     *
     * Additionally, this means that we only need to store the data for a single access point in RAM at a time,
     * which overcomes the potential issue of available RAM on the benchmark-host.
     */
    List<KMeans> computeKMeans(LocalDateTime start, LocalDateTime end, int numClusters, int numIterations) throws IOException, SQLException;
}
