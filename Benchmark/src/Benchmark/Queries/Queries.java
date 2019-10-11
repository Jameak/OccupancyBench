package Benchmark.Queries;

import Benchmark.Config.ConfigFile;
import Benchmark.Generator.Floor;

import java.time.LocalDateTime;

public interface Queries {
    void prepare(ConfigFile config) throws Exception;
    void done();

    int computeTotalClients(LocalDateTime start, LocalDateTime end);
    int[] computeFloorTotal(LocalDateTime start, LocalDateTime end, Floor[] generatedFloors);
}
