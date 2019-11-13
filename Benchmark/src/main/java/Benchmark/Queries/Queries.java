package Benchmark.Queries;

import Benchmark.Config.ConfigFile;
import Benchmark.Generator.GeneratedData.AccessPoint;
import Benchmark.Generator.GeneratedData.Floor;

import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.List;

/**
 * A shared interface for all Query-implementations.
 */
public interface Queries {
    void prepare(ConfigFile config, Floor[] generatedFloors) throws Exception;
    void done() throws SQLException;
    LocalDateTime getNewestTimestamp() throws SQLException;

    int computeTotalClients(LocalDateTime start, LocalDateTime end) throws SQLException;
    int[] computeFloorTotal(LocalDateTime start, LocalDateTime end, Floor[] generatedFloors) throws SQLException;
    int[] maxPerDayForAP(LocalDateTime start, LocalDateTime end, AccessPoint AP) throws SQLException;
    List<AvgOccupancy> computeAvgOccupancy(LocalDateTime start, LocalDateTime end, int windowSizeInMin) throws SQLException;

    class AvgOccupancy{
        private final String AP;
        private final int currentClients;
        private final double historical_clients_now;
        private final double historical_clients_soon;

        public AvgOccupancy(String AP, int currentClients, double historical_clients_now, double historical_clients_soon){
            this.AP = AP;
            this.currentClients = currentClients;
            this.historical_clients_now = historical_clients_now;
            this.historical_clients_soon = historical_clients_soon;
        }

        public String getAP() {
            return AP;
        }

        public int getCurrentClients() {
            return currentClients;
        }

        public double getHistorical_clients_now() {
            return historical_clients_now;
        }

        public double getHistorical_clients_soon() {
            return historical_clients_soon;
        }

        @Override
        public String toString() {
            return AP + "; " + currentClients + "; " + historical_clients_now + "; " + historical_clients_soon;
        }
    }
}
