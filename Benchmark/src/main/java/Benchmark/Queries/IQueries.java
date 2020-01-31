package Benchmark.Queries;

import Benchmark.Config.ConfigFile;
import Benchmark.Generator.GeneratedData.AccessPoint;
import Benchmark.Generator.GeneratedData.Floor;
import Benchmark.Queries.Results.*;

import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.List;

/**
 * A shared interface for all Query-implementations.
 */
public interface IQueries {
    void prepare(ConfigFile config, Floor[] generatedFloors) throws Exception;
    void done() throws SQLException;
    LocalDateTime getNewestTimestamp() throws SQLException;

    List<Total> computeTotalClients(LocalDateTime start, LocalDateTime end) throws SQLException;
    List<FloorTotal> computeFloorTotal(LocalDateTime start, LocalDateTime end, Floor[] generatedFloors) throws SQLException;
    List<MaxForAP> maxPerDayForAP(LocalDateTime start, LocalDateTime end, AccessPoint AP) throws SQLException;
    List<AvgOccupancy> computeAvgOccupancy(LocalDateTime start, LocalDateTime end, int windowSizeInMin) throws SQLException;
}
