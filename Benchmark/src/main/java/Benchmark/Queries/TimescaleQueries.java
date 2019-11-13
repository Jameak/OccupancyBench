package Benchmark.Queries;

import Benchmark.Config.ConfigFile;
import Benchmark.Generator.GeneratedData.AccessPoint;
import Benchmark.Generator.GeneratedData.Floor;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * An implementation of the benchmark-queries for TimescaleDB.
 */
public class TimescaleQueries extends JdbcQueries {
    private String table;
    private int sampleRate;
    private Map<Integer, String> precomputedFloorTotalQueryParts = new HashMap<>();

    @Override
    public void prepare(ConfigFile config, Floor[] generatedFloors) throws Exception {
        this.table = config.getTimescaleTable();
        this.sampleRate = config.getGeneratorGenerationInterval();

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

            sb.append(") GROUP BY bucket");
            precomputedFloorTotalQueryParts.put(floor.getFloorNumber(), sb.toString());
        }
    }

    @Override
    public LocalDateTime getNewestTimestamp() throws SQLException {
        String queryString = String.format("SELECT * FROM %s ORDER BY time DESC LIMIT 1", table);

        String time = null;
        try(Statement statement = connection.createStatement();
            ResultSet results = statement.executeQuery(queryString)){
            while(results.next()) {
                time = results.getString("time");
            }
        }

        assert time != null;
        String[] parts = time.split(" ");
        return LocalDateTime.of(LocalDate.parse(parts[0]), LocalTime.parse(parts[1]));
    }

    @Override
    public int computeTotalClients(LocalDateTime start, LocalDateTime end) throws SQLException {
        double timeStart = toTimestamp(start);
        double timeEnd = toTimestamp(end);

        String query = String.format("SELECT time_bucket('1 day', time) AS bucket, SUM(clients) " +
                "FROM %s " +
                "WHERE time >= TO_TIMESTAMP(%s) AND time < TO_TIMESTAMP(%s) " +
                "GROUP BY bucket", table, timeStart, timeEnd);

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
        double timeStart = toTimestamp(start);
        double timeEnd = toTimestamp(end);

        int[] counts = new int[generatedFloors.length];
        for(int i = 0; i < generatedFloors.length; i++){
            Floor floor = generatedFloors[i];
            String query = String.format("SELECT time_bucket('1 day', time) AS bucket, SUM(clients) " +
                            "FROM %s " +
                            "WHERE time >= TO_TIMESTAMP(%s) AND time < TO_TIMESTAMP(%s) AND %s",
                    table, timeStart, timeEnd, precomputedFloorTotalQueryParts.get(floor.getFloorNumber()));

            try(Statement statement = connection.createStatement();
                ResultSet results = statement.executeQuery(query)){
                while(results.next()) {
                    // I assume we need to page through the result-set to guarantee that we pull all results from the database.
                }
            }
        }

        return counts;
    }

    @Override
    public int[] maxPerDayForAP(LocalDateTime start, LocalDateTime end, AccessPoint AP) throws SQLException {
        double timeStart = toTimestamp(start);
        double timeEnd = toTimestamp(end);

        String query = String.format("SELECT time_bucket('1 day', time) AS bucket, MAX(clients) " +
                "FROM %s " +
                "WHERE AP='%s' AND time >= TO_TIMESTAMP(%s) AND time < TO_TIMESTAMP(%s) " +
                "GROUP BY bucket", table, AP.getAPname(), timeStart, timeEnd);

        try(Statement statement = connection.createStatement();
            ResultSet results = statement.executeQuery(query)){
            while(results.next()) {
                // I assume we need to page through the result-set to guarantee that we pull all results from the database.
            }
        }

        return new int[0];
    }

    @Override
    public List<AvgOccupancy> computeAvgOccupancy(LocalDateTime start, LocalDateTime end, int windowSizeInMin) throws SQLException {
        StringBuilder sb1 = new StringBuilder();
        StringBuilder sb2 = new StringBuilder();

        {
            LocalDateTime date = end;
            boolean firstLoop = true;
            do{
                if(firstLoop) firstLoop = false;
                else sb1.append(" OR ");

                String time = String.format("(time <= TO_TIMESTAMP(%s) AND time > TO_TIMESTAMP(%s))", toTimestamp(date), toTimestamp(date.minusMinutes(windowSizeInMin)));
                sb1.append(time);

                date = date.minusDays(1);
            } while(!date.isBefore(start));
        }
        {
            LocalDateTime date = end;
            boolean firstLoop = true;
            do{
                if(firstLoop) firstLoop = false;
                else sb2.append(" OR ");

                String time = String.format("(time <= TO_TIMESTAMP(%s) AND time > TO_TIMESTAMP(%s))", toTimestamp(date.plusMinutes(windowSizeInMin)), toTimestamp(date));
                sb2.append(time);

                date = date.minusDays(1);
            } while(!date.isBefore(start));
        }

        String query = String.format(
                "SELECT now.AP, " +
                        "FIRST(now.client_entry, now.time) AS current_clients, " +
                        "AVG(now_stat.avg_clients) AS historical_clients_now, " +
                        "AVG(past_soon.avg_clients) AS historical_clients_soon " +
                "FROM (" +
                        "SELECT AP, AVG(clients) AS avg_clients " +
                        "FROM %s " +
                        "WHERE %s " +
                        "GROUP BY AP " +
                ") AS now_stat " +
                "INNER JOIN (" +
                        "SELECT AP, AVG(clients) as avg_clients " +
                        "FROM %s " +
                        "WHERE %s " +
                        "GROUP BY AP " +
                ") AS past_soon " +
                "ON now_stat.ap = past_soon.ap " +
                "INNER JOIN (" +
                        "SELECT AP, clients as client_entry, time " +
                        "FROM %s " +
                        "WHERE time <= TO_TIMESTAMP(%s) AND time > TO_TIMESTAMP(%s) " +
                ") AS now " +
                "ON now.ap = now_stat.ap " +
                "GROUP BY now.AP", table, sb1.toString(), table, sb2.toString(), table, toTimestamp(end), toTimestamp(end.minusSeconds(sampleRate)));

        List<AvgOccupancy> output = new ArrayList<>();
        try(Statement statement = connection.createStatement();
            ResultSet results = statement.executeQuery(query)){
            while(results.next()) {
                String AP = results.getString("AP");
                int currentClients = results.getInt("current_clients");
                double historicalClientsNow = results.getDouble("historical_clients_now");
                double historicalClientsSoon = results.getDouble("historical_clients_soon");
                output.add(new AvgOccupancy(AP, currentClients, historicalClientsNow, historicalClientsSoon));
            }
        }

        return output;
    }

    private double toTimestamp(LocalDateTime time){
        return time.toInstant(ZoneOffset.ofHours(0)).toEpochMilli() / 1e3;
    }
}
