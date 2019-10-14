package Benchmark.Queries;

import Benchmark.Config.ConfigFile;
import Benchmark.Generator.GeneratedData.Floor;

import java.time.LocalDateTime;

/**
 * A shared interface for all Query-implementations.
 */
public interface Queries {
    void prepare(ConfigFile config, Floor[] generatedFloors) throws Exception;
    void done();

    int computeTotalClients(LocalDateTime start, LocalDateTime end);
    int[] computeFloorTotal(LocalDateTime start, LocalDateTime end, Floor[] generatedFloors);
}
