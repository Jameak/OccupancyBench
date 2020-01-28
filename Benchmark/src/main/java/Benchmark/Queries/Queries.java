package Benchmark.Queries;

import Benchmark.Config.ConfigFile;
import Benchmark.Generator.GeneratedData.AccessPoint;
import Benchmark.Generator.GeneratedData.Floor;

import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Locale;

/**
 * A shared interface for all Query-implementations.
 */
public interface Queries {
    void prepare(ConfigFile config, Floor[] generatedFloors) throws Exception;
    void done() throws SQLException;
    LocalDateTime getNewestTimestamp() throws SQLException;

    List<Total> computeTotalClients(LocalDateTime start, LocalDateTime end) throws SQLException;
    List<FloorTotal> computeFloorTotal(LocalDateTime start, LocalDateTime end, Floor[] generatedFloors) throws SQLException;
    List<MaxForAP> maxPerDayForAP(LocalDateTime start, LocalDateTime end, AccessPoint AP) throws SQLException;
    List<AvgOccupancy> computeAvgOccupancy(LocalDateTime start, LocalDateTime end, int windowSizeInMin) throws SQLException;

    abstract class Result{
        public abstract String print();

        @Override
        public final String toString() {
            return print();
        }
    }

    class Total extends Result{
        private final String dayTimestamp;
        private final int total;

        public Total(String dayTimestamp, int total){
            this.dayTimestamp = dayTimestamp;
            this.total = total;
        }

        @Override
        public String print() {
            return dayTimestamp + ";" + total;
        }
    }

    class FloorTotal extends Result {
        private final int floor;
        private final String dayTimestamp;
        private final int total;

        public FloorTotal(int floor, String dayTimestamp, int total){
            this.floor = floor;
            this.dayTimestamp = dayTimestamp;
            this.total = total;
        }

        @Override
        public String print() {
            return dayTimestamp + ";" + floor + ";" + total;
        }
    }

    class MaxForAP extends Result {
        private final String AP;
        private final String time;
        private final int maxVal;

        public MaxForAP(String AP, String time, int maxVal){
            this.AP = AP;
            this.time = time;
            this.maxVal = maxVal;
        }

        @Override
        public String print() {
            return AP + ";" + time + ";" + maxVal;
        }
    }

    class AvgOccupancy extends Result{
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

        @Override
        public String print() {
            return String.format(Locale.US, "%s;%d;%.2f;%.2f", AP, currentClients, historical_clients_now, historical_clients_soon);
        }
    }
}
