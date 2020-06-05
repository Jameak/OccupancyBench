package Benchmark.Debug;

import Benchmark.Config.ConfigFile;
import Benchmark.Databases.Timescale.AbstractTimescaleQueries;
import Benchmark.Databases.Timescale.TimescaleHelper;
import Benchmark.Generator.GeneratedData.GeneratedAccessPoint;
import Benchmark.Generator.GeneratedData.GeneratedFloor;
import Benchmark.Queries.QueryHelper;
import Benchmark.Queries.Results.*;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.util.*;

/**
 * Specialized implementation of the Timescale query-implementation for the column schema.
 *
 * It uses the EXPLAIN ANALYZE Postgres statements to retrieve planning- and execution-time
 * information about each query.
 */
public class PartitionLockstepTimescaleDetailedColumnQueries extends AbstractTimescaleQueries {
    private final PartitionLockstepChannel channel;

    private String precomputedTotalClientsPart;
    private String precomputedFloorTotalPart;
    private String precomputedAvgOccupancyPart1;
    private String precomputedAvgOccupancyPart2;
    private GeneratedAccessPoint[] allAPs;
    private int sampleRate;

    private int queriesSinceLastStop = 0;

    public PartitionLockstepTimescaleDetailedColumnQueries(PartitionLockstepChannel channel){
        this.channel = channel;
    }

    private PartitionLockstepChannel.QueryDuration runAndParse(String queryName, String query) throws SQLException{
        ArrayList<String> lines = new ArrayList<>();

        try(Statement statement = connection.createStatement();
            ResultSet results = statement.executeQuery(query)){
            while(results.next()) {
                lines.add(results.getString(1));
            }
        }

        String planningTime = lines.get(lines.size()-2);
        String executionTime = lines.get(lines.size()-1);
        return new PartitionLockstepChannel.QueryDuration(queryName, planningTime, executionTime);
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
        this.table = config.getTimescaleTable();
        this.sampleRate = config.getGeneratorGenerationSamplerate();
        this.connection = TimescaleHelper.openConnection(config.getTimescaleUsername(), config.getTimescalePassword(), config.getTimescaleHost(), config.getTimescaleDBName(), false);
        this.allAPs = GeneratedFloor.allAPsOnFloors(generatedFloors);

        precomputedTotalClientsPart = QueryHelper.buildColumnSchemaTotalClientsQueryPrecomputation(allAPs);
        precomputedFloorTotalPart = QueryHelper.buildColumnSchemaFloorTotalQueryPrecomputation(generatedFloors);
        precomputedAvgOccupancyPart1 = QueryHelper.buildColumnSchemaAvgOccupancyPrecomputation_AVG("AVG", allAPs);
        precomputedAvgOccupancyPart2 = QueryHelper.buildColumnSchemaAvgOccupancyPrecomputation_SELECT_ALL(allAPs);
    }

    @Override
    public List<Total> computeTotalClients(LocalDateTime start, LocalDateTime end) throws SQLException {
        long timeStart = toTimestamp(start);
        long timeEnd = toTimestamp(end);

        String query = String.format("EXPLAIN ANALYZE SELECT time_bucket('1 day', time) AS bucket, %s " +
                "FROM %s " +
                "WHERE time > TO_TIMESTAMP(%s) AND time <= TO_TIMESTAMP(%s) " +
                "GROUP BY bucket", precomputedTotalClientsPart, table, timeStart, timeEnd);

        PartitionLockstepChannel.QueryDuration duration = runAndParse("Total Clients", query);
        writeAndAwait(duration);

        return null;
    }

    @Override
    public List<FloorTotal> computeFloorTotal(LocalDateTime start, LocalDateTime end) throws SQLException {
        long timeStart = toTimestamp(start);
        long timeEnd = toTimestamp(end);

        String query = String.format("EXPLAIN ANALYZE SELECT time_bucket('1 day', time) AS bucket, %s " +
                        "FROM %s " +
                        "WHERE time > TO_TIMESTAMP(%s) AND time <= TO_TIMESTAMP(%s)" +
                        "GROUP BY bucket",
                precomputedFloorTotalPart, table, timeStart, timeEnd);

        PartitionLockstepChannel.QueryDuration duration = runAndParse("Floor Total", query);
        writeAndAwait(duration);

        return null;
    }

    @Override
    public List<MaxForAP> maxPerDayForAP(LocalDateTime start, LocalDateTime end, GeneratedAccessPoint AP) throws SQLException {
        long timeStart = toTimestamp(start);
        long timeEnd = toTimestamp(end);

        String query = String.format("EXPLAIN ANALYZE SELECT time_bucket('1 day', time) AS bucket, MAX(\"%s\") " +
                "FROM %s " +
                "WHERE time > TO_TIMESTAMP(%s) AND time <= TO_TIMESTAMP(%s) " +
                "GROUP BY bucket", AP.getAPname(), table, timeStart, timeEnd);

        PartitionLockstepChannel.QueryDuration duration = runAndParse("Max for AP", query);
        writeAndAwait(duration);

        return null;
    }

    @Override
    public List<AvgOccupancy> computeAvgOccupancy(LocalDateTime start, LocalDateTime end, int windowSizeInMin) throws SQLException {
        StringBuilder sb1 = new StringBuilder();
        StringBuilder sb2 = new StringBuilder();
        buildAvgOccupancyTimeranges(start, end, sb1, sb2, windowSizeInMin);

        String query1 = String.format(
                "EXPLAIN ANALYZE SELECT %s " +
                        "FROM %s " +
                        "WHERE %s",
                precomputedAvgOccupancyPart1, table, sb1.toString());

        String query2 = String.format(
                "EXPLAIN ANALYZE SELECT %s " +
                        "FROM %s " +
                        "WHERE %s",
                precomputedAvgOccupancyPart1, table, sb2.toString());

        String query3 = String.format(
                "EXPLAIN ANALYZE SELECT %s " +
                        "FROM %s " +
                        "WHERE time > TO_TIMESTAMP(%s) AND time <= TO_TIMESTAMP(%s)",
                precomputedAvgOccupancyPart2, table, toTimestamp(end.minusSeconds(sampleRate)), toTimestamp(end));


        int totalPlanningTime = 0;
        int totalExecutionTime = 0;
        PartitionLockstepChannel.QueryDuration duration = runAndParse("Avg Occupancy", query1);
        totalPlanningTime += duration.planningTime;
        totalExecutionTime += duration.executionTime;

        duration = runAndParse("Avg Occupancy", query2);
        totalPlanningTime += duration.planningTime;
        totalExecutionTime += duration.executionTime;

        duration = runAndParse("Avg Occupancy", query3);
        totalPlanningTime += duration.planningTime;
        totalExecutionTime += duration.executionTime;

        duration = new PartitionLockstepChannel.QueryDuration("Avg Occupancy", totalPlanningTime, totalExecutionTime);
        writeAndAwait(duration);

        return null;
    }

    @Override
    public List<KMeans> computeKMeans(LocalDateTime start, LocalDateTime end, int numClusters, int numIterations) throws SQLException {
        long timeStart = toTimestamp(start);
        long timeEnd = toTimestamp(end);

        int totalPlanningTime = 0;
        int totalExecutionTime = 0;

        for(int i = 0; i < numIterations; i++){
            for(GeneratedAccessPoint AP : allAPs){
                String query = String.format("EXPLAIN ANALYZE SELECT time, \"%s\" " +
                                "FROM %s " +
                                "WHERE time > TO_TIMESTAMP(%s) AND time <= TO_TIMESTAMP(%s) " +
                                "ORDER BY time ASC",
                        AP.getAPname(), table, timeStart, timeEnd);

                PartitionLockstepChannel.QueryDuration duration = runAndParse("K-Means", query);
                totalPlanningTime += duration.planningTime;
                totalExecutionTime += duration.executionTime;
            }
        }

        PartitionLockstepChannel.QueryDuration duration = new PartitionLockstepChannel.QueryDuration("K-Means", totalPlanningTime, totalExecutionTime);
        writeAndAwait(duration);

        return null;
    }
}
