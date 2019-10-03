package Benchmark.Analysis;

import Benchmark.Generator.AccessPoint;
import Benchmark.Generator.Floor;
import Benchmark.Generator.MapData;
import org.influxdb.BatchOptions;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class Precomputation {
    public static void ComputeTotals(String url, String username, String password, String dbName,
                                     String measurementName, int interval, Floor[] generatedFloors,
                                     LocalDate startDate, LocalDate endDate) throws IOException {
        InfluxDB influxDB = SetupConnection(url, username, password, dbName);

        LocalDate nextDate = startDate;
        while(nextDate.isBefore(endDate)){
            LocalDateTime nextTime = LocalDateTime.of(nextDate, LocalTime.of(0, 0, 0, 0));
            LocalDateTime endTime = LocalDateTime.of(nextDate.plusDays(1), LocalTime.of(0, 0, 0, 0));

            while(nextTime.isBefore(endTime)){
                ComputeTotalForTime(influxDB, measurementName, nextTime, interval, generatedFloors);
                nextTime = nextTime.plusSeconds(interval);
            }

            nextDate = nextDate.plusDays(1);
        }

        influxDB.close();
    }

    private static void ComputeTotalForTime(InfluxDB db, String measurement, LocalDateTime time, int interval, Floor[] generatedFloors){
        assert interval > 3 : "If readings are too close together than the next reading starts before the previous one ends, so defining a total for an interval would be much more annoying";
        String precomputedTotalName = "total";
        String precomputedFloorName = "floor";
        int total = 0;
        Map<Integer, Integer> floorTotals = new HashMap<>();
        for(Floor floor : generatedFloors){
            floorTotals.put(floor.getFloorNumber(), 0);
        }
        String queryString = "SELECT AP,clients FROM " + measurement + " WHERE time < '" + ToStringTime(time.plusSeconds(interval).minusSeconds(1)) + "' AND time > '" + ToStringTime(time) + "'";
        Query query = new Query(queryString);
        QueryResult results = db.query(query);
        for(QueryResult.Result result : results.getResults()){
            if(result.getSeries() == null) continue; // No results. Caused by hole in data.
            for(QueryResult.Series series : result.getSeries()){
                for(List<Object> entries : series.getValues()){
                    String AP = (String)entries.get(series.getColumns().indexOf("AP"));
                    int clients = (int)Math.round((Double)entries.get(series.getColumns().indexOf("clients")));

                    total += clients;
                    int floorNum = GetFloorForAP(AP, generatedFloors);
                    floorTotals.put(floorNum, floorTotals.get(floorNum) + clients);
                }
            }
        }

        Instant timeInstant = Instant.parse(ToStringTime(time));
        long timeNano = timeInstant.getEpochSecond() * 1_000_000_000 + timeInstant.getNano();

        db.write(
                Point.measurement(precomputedTotalName)
                        .time(timeNano, TimeUnit.NANOSECONDS)
                        .addField("total", total)
                        .build());

        for(Floor floor : generatedFloors){
            db.write(
                    Point.measurement(precomputedFloorName + floor.getFloorNumber())
                            .time(timeNano, TimeUnit.NANOSECONDS)
                            .addField("total", floorTotals.get(floor.getFloorNumber()))
                            .build());
        }
    }

    private static int GetFloorForAP(String AP, Floor[] generatedFloors){
        for (Floor floor : generatedFloors) {
            for(AccessPoint floorAP : floor.getAPs()){
                if(floorAP.getAPname().equals(AP)){
                    return floor.getFloorNumber();
                }
            }
        }

        assert false : "AP found that doesn't belong to a floor. WTF";
        return -1;
    }


    private static String ToStringTime(LocalDateTime time){
        // TODO: For the query used in the benchmark, I assume that passing this as the nano-second precision number would be more performant.
        return String.format("%d-%02d-%02dT%02d:%02d:%02dZ", time.getYear(), time.getMonthValue(), time.getDayOfMonth(), time.getHour(), time.getMinute(), time.getSecond());
        //return time.getYear() + "-" + time.getMonthValue() + "-" + time.getDayOfMonth() + "T" + time.getHour() + ":" + time.getMinute() + ":" + time.getSecond() + "Z";
    }

    private static InfluxDB SetupConnection(String url, String username, String password, String dbName) throws IOException{
        InfluxDB influxDB = InfluxDBFactory.connect(url, username, password);
        if (influxDB.ping().getVersion().equalsIgnoreCase("unknown")) {
            influxDB.close();
            throw new IOException("No connection to Influx database.");
        }

        influxDB.setDatabase(dbName);
        influxDB.enableBatch(BatchOptions.DEFAULTS);
        influxDB.query(new Query("DROP MEASUREMENT total"));
        influxDB.query(new Query("DROP MEASUREMENT floor0"));
        influxDB.query(new Query("DROP MEASUREMENT floor1"));
        influxDB.query(new Query("DROP MEASUREMENT floor2"));
        influxDB.query(new Query("DROP MEASUREMENT floor3"));
        influxDB.query(new Query("DROP MEASUREMENT floor4"));
        influxDB.query(new Query("DROP MEASUREMENT floor5"));
        influxDB.query(new Query("DROP MEASUREMENT floor6"));
        influxDB.query(new Query("DROP MEASUREMENT floor7"));
        influxDB.query(new Query("DROP MEASUREMENT floor8"));
        influxDB.query(new Query("DROP MEASUREMENT floor9"));
        influxDB.query(new Query("DROP MEASUREMENT floor10"));
        influxDB.query(new Query("DROP MEASUREMENT floor11"));
        influxDB.query(new Query("DROP MEASUREMENT floor12"));
        influxDB.query(new Query("DROP MEASUREMENT floor13"));
        influxDB.query(new Query("DROP MEASUREMENT floor14"));
        influxDB.query(new Query("DROP MEASUREMENT floor15"));
        return influxDB;
    }
}
