package Benchmark.Queries;

import Benchmark.Config.ConfigFile;
import Benchmark.Generator.GeneratedData.AccessPoint;
import Benchmark.Generator.GeneratedData.Floor;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;

public class TimescaleQueries extends JdbcQueries {
    private String table;
    private Map<Integer, String> precomputedFloorTotalQueryParts = new HashMap<>();
    private int timeBetweenReadings;

    @Override
    public void prepare(ConfigFile config, Floor[] generatedFloors) throws Exception {
        this.timeBetweenReadings = config.getGeneratorGenerationInterval();
        this.table = config.getTimescaleTable();

        DriverManager.registerDriver(new org.postgresql.Driver());
        // TODO: Include SSL-parameter in the connection string?
        String db = String.format("jdbc:postgresql://%s/%s", config.getTimescaleHost(), config.getTimescaleDBName());
        connection = DriverManager.getConnection(db, config.getTimescaleUsername(), config.getTimescalePassword());

        //Pre-computations of the strings needed for the 'FloorTotal' query to minimize time spent in Java code versus on the query itself:
        for(Floor floor : generatedFloors){
            StringBuilder sb = new StringBuilder();
            sb.append("(");
            boolean first = true;
            for(AccessPoint AP : floor.getAPs()){
                if(first){
                    first = false;
                } else {
                    sb.append("OR");
                }
                sb.append(" AP='");
                sb.append(AP.getAPname());
                sb.append("' ");
            }

            sb.append(") GROUP BY bucket ORDER BY bucket ASC");
            precomputedFloorTotalQueryParts.put(floor.getFloorNumber(), sb.toString());
        }
    }

    @Override
    public int computeTotalClients(LocalDateTime start, LocalDateTime end) throws SQLException {
        double timeStart = start.toInstant(ZoneOffset.ofHours(0)).toEpochMilli() / 1e3;
        double timeEnd = end.toInstant(ZoneOffset.ofHours(0)).toEpochMilli() / 1e3;

        String query = String.format("SELECT time_bucket('%s seconds', time) AS bucket, SUM(clients) " +
                "FROM %s " +
                "WHERE time >= TO_TIMESTAMP(%s) AND time < TO_TIMESTAMP(%s) " +
                "GROUP BY bucket ORDER BY bucket ASC", timeBetweenReadings, table, timeStart, timeEnd);

        try(Statement statement = connection.createStatement();
            ResultSet results = statement.executeQuery(query)){
            while(results.next()) {
                // I assume we need to page through the result-set to guarantee that we pull all results from the database.
            }
        }

        return 0;
    }

    @Override
    public int[] computeFloorTotal(LocalDateTime start, LocalDateTime end, Floor[] generatedFloors) throws SQLException {
        double timeStart = start.toInstant(ZoneOffset.ofHours(0)).toEpochMilli() / 1e3;
        double timeEnd = end.toInstant(ZoneOffset.ofHours(0)).toEpochMilli() / 1e3;

        int[] counts = new int[generatedFloors.length];
        for(int i = 0; i < generatedFloors.length; i++){
            Floor floor = generatedFloors[i];
            String query = String.format("SELECT time_bucket('%s seconds', time) AS bucket, SUM(clients) " +
                            "FROM %s " +
                            "WHERE time >= TO_TIMESTAMP(%s) AND time < TO_TIMESTAMP(%s) AND %s",
                    timeBetweenReadings, table, timeStart, timeEnd, precomputedFloorTotalQueryParts.get(floor.getFloorNumber()));

            try(Statement statement = connection.createStatement();
                ResultSet results = statement.executeQuery(query)){
                while(results.next()) {
                    // I assume we need to page through the result-set to guarantee that we pull all results from the database.
                }
            }
        }

        return counts;
    }
}
