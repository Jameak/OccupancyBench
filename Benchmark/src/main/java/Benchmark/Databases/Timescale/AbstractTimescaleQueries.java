package Benchmark.Databases.Timescale;

import Benchmark.Queries.IQueries;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;

/**
 * Abstract class for Timescale query-implementations containing default-implementations and convenience-functions.
 */
public abstract class AbstractTimescaleQueries implements IQueries {
    protected Connection connection;
    protected String table;

    @Override
    public LocalDateTime getNewestTimestamp(LocalDateTime previousNewestTime) throws SQLException {
        String queryString = String.format("SELECT * FROM %s ORDER BY time DESC LIMIT 1", table);

        String time = null;
        try(Statement statement = connection.createStatement();
            ResultSet results = statement.executeQuery(queryString)){
            while(results.next()) {
                time = results.getString("time");
            }
        }

        // If the database is empty, just return the old value
        if(time == null){
            return previousNewestTime;
        }

        String[] parts = time.split(" ");
        return LocalDateTime.of(LocalDate.parse(parts[0]), LocalTime.parse(parts[1]));
    }

    @Override
    public void done() throws SQLException {
        if(connection == null) return;
        connection.close();
    }

    protected long toTimestamp(LocalDateTime time){
        // Timestamps in Timescale (when using TO_TIMESTAMP) expect second precision
        return (long) (time.toInstant(ZoneOffset.ofHours(0)).toEpochMilli() / 1e3);
    }

    protected void buildAvgOccupancyTimeranges(LocalDateTime start, LocalDateTime end, StringBuilder sb1, StringBuilder sb2, int windowSizeInMin){
        {
            LocalDateTime date = end;
            boolean firstLoop = true;
            do{
                if(firstLoop) firstLoop = false;
                else sb1.append(" OR ");

                String time = String.format("(time > TO_TIMESTAMP(%s) AND time <= TO_TIMESTAMP(%s))", toTimestamp(date.minusMinutes(windowSizeInMin)), toTimestamp(date));
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

                String time = String.format("(time > TO_TIMESTAMP(%s) AND time <= TO_TIMESTAMP(%s))", toTimestamp(date), toTimestamp(date.plusMinutes(windowSizeInMin)));
                sb2.append(time);

                date = date.minusDays(1);
            } while(!date.isBefore(start));
        }
    }
}
