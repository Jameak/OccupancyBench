package Benchmark.Queries;

import Benchmark.Config.ConfigFile;
import Benchmark.Generator.GeneratedData.AccessPoint;
import Benchmark.Generator.GeneratedData.Floor;

import java.sql.SQLException;
import java.time.LocalDateTime;

/**
 * A shared interface for all Query-implementations.
 */
public interface Queries {
    void prepare(ConfigFile config, Floor[] generatedFloors) throws Exception;
    void done() throws SQLException;

    int computeTotalClients(LocalDateTime start, LocalDateTime end) throws SQLException;
    int[] computeFloorTotal(LocalDateTime start, LocalDateTime end, Floor[] generatedFloors) throws SQLException;
    int[] maxPerDayForAP(LocalDateTime start, LocalDateTime end, AccessPoint AP) throws SQLException;
}
