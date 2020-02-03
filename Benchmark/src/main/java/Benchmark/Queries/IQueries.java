package Benchmark.Queries;

import Benchmark.Config.ConfigFile;
import Benchmark.Generator.GeneratedData.AccessPoint;
import Benchmark.Generator.GeneratedData.Floor;
import Benchmark.Queries.Results.*;

import java.io.IOException;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.List;

/**
 * A shared interface for all Query-implementations.
 */
public interface IQueries {
    /**
     * This method is called once by the framework, before any queries are executed.
     * Use it to perform whatever pre-computations are possible.
     */
    void prepare(ConfigFile config, Floor[] generatedFloors) throws Exception;

    /**
     * This method is called once by the framework, after querying is finished.
     * Use it to cleanup resources.
     */
    void done() throws IOException, SQLException;

    /**
     * Query the database for the newest available timestamp and return it.
     * This allows the database to make decisions regarding staleness of data.
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
    List<MaxForAP> maxPerDayForAP(LocalDateTime start, LocalDateTime end, AccessPoint AP) throws IOException, SQLException;

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
}
