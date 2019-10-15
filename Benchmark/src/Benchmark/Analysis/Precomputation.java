package Benchmark.Analysis;

import Benchmark.Config.ConfigFile;
import Benchmark.Generator.GeneratedData.AccessPoint;
import Benchmark.Generator.GeneratedData.Floor;
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

/**
 * A class dedicated to performing computations on generated data and saving it in a different database for later
 * referencing. The data that is saved resembles the pre-computations done to ITU data, and various metadata is also
 * included that would otherwise be lost once the original database is deleted.
 *
 * This data is useful for debugging and for comparisons between the actual ITU data and the generated data, and between
 * generated data that was generated with different config settings.
 */
public class Precomputation {
    public static void ComputeTotals(int interval, Floor[] generatedFloors, ConfigFile config) throws IOException {
        InfluxDB writeDB = SetupConnection(config.getInfluxUrl(), config.getInfluxUsername(), config.getInfluxPassword(), config, false);
        InfluxDB readDB = SetupConnection(config.getInfluxUrl(), config.getInfluxUsername(), config.getInfluxPassword(), config, true);

        LocalDate nextDate = config.getGeneratorStartDate();
        while(nextDate.isBefore(config.getGeneratorEndDate())){
            LocalDateTime nextTime = LocalDateTime.of(nextDate, LocalTime.of(0, 0, 0, 0));
            LocalDateTime endTime = LocalDateTime.of(nextDate.plusDays(1), LocalTime.of(0, 0, 0, 0));

            while(nextTime.isBefore(endTime)){
                ComputeTotalForTime(readDB, writeDB, config.getInfluxTable(), nextTime, interval, generatedFloors, config);
                nextTime = nextTime.plusSeconds(interval);
            }

            nextDate = nextDate.plusDays(1);
        }

        WriteMeta(readDB, writeDB, config, generatedFloors);

        writeDB.close();
        readDB.close();
    }

    private static void WriteMeta(InfluxDB readDB, InfluxDB writeDB, ConfigFile config, Floor[] generatedFloors){
        String tablePrefix = GetTablePrefix(config);
        String precomputedMetaName = tablePrefix + "Meta";

        Query query = new Query("SELECT COUNT(clients) FROM " + config.getInfluxTable());
        QueryResult results = readDB.query(query);
        int count = 0;
        for(QueryResult.Result result : results.getResults()){
            assert result.getSeries() != null;
            for(QueryResult.Series series : result.getSeries()){
                for(List<Object> entries : series.getValues()){
                    count = (int)Math.round((Double) entries.get(series.getColumns().indexOf("count")));
                }
            }
        }

        writeDB.write(
                Point.measurement(precomputedMetaName)
                        .time(150, TimeUnit.NANOSECONDS)
                        .addField("info", count)
                        .build());

        int numAPs = 0;
        for(Floor floor : generatedFloors){
            numAPs += floor.getAPs().length;
        }
        writeDB.write(
                Point.measurement(precomputedMetaName)
                        .time(300, TimeUnit.NANOSECONDS)
                        .addField("info", numAPs)
                        .build());

        writeDB.write(
                Point.measurement(precomputedMetaName)
                        .time(450, TimeUnit.NANOSECONDS)
                        .addField("info", generatedFloors.length)
                        .build());
    }

    private static void ComputeTotalForTime(InfluxDB readDB, InfluxDB writeDB, String measurement, LocalDateTime time, int interval, Floor[] generatedFloors, ConfigFile config){
        assert interval > 3 : "If readings are too close together than the next reading starts before the previous one ends, so defining a total for an interval would be much more annoying";
        String tablePrefix = GetTablePrefix(config);
        String precomputedTotalName = tablePrefix + "Total";
        String precomputedFloorName = tablePrefix + "Floor";
        int total = 0;
        Map<Integer, Integer> floorTotals = new HashMap<>();
        for(Floor floor : generatedFloors){
            floorTotals.put(floor.getFloorNumber(), 0);
        }
        String queryString = "SELECT AP,clients FROM " + measurement + " WHERE time < '" + ToStringTime(time.plusSeconds(interval).minusSeconds(1)) + "' AND time > '" + ToStringTime(time) + "'";
        Query query = new Query(queryString);
        QueryResult results = readDB.query(query);
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

        writeDB.write(
                Point.measurement(precomputedTotalName)
                        .time(timeNano, TimeUnit.NANOSECONDS)
                        .addField("total", total)
                        .build());

        for(Floor floor : generatedFloors){
            writeDB.write(
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
    }

    private static String GetTablePrefix(ConfigFile config){
        String tablePrefix = "Int" + config.getGeneratorGenerationInterval() + "_";
        if(!config.keepFloorAssociationsForGenerator()) tablePrefix += "NoAssoc_";
        tablePrefix += ("Scale" + config.getGeneratorScale()).replace(".", "_").replace(",", "_") + "_";
        return tablePrefix;
    }

    private static InfluxDB SetupConnection(String url, String username, String password, ConfigFile config, boolean readDB) throws IOException{
        InfluxDB influxDB = InfluxDBFactory.connect(url, username, password);
        if (influxDB.ping().getVersion().equalsIgnoreCase("unknown")) {
            influxDB.close();
            throw new IOException("No connection to Influx database.");
        }

        if(readDB){
            influxDB.setDatabase(config.getInfluxDBName());
            influxDB.enableBatch(BatchOptions.DEFAULTS);
            return influxDB;
        }

        String tablePrefix = GetTablePrefix(config);
        influxDB.query(new Query("CREATE DATABASE debug_analytics"));

        influxDB.setDatabase("debug_analytics");
        influxDB.enableBatch(BatchOptions.DEFAULTS);
        influxDB.query(new Query("DROP MEASUREMENT " + tablePrefix + "Total"));
        influxDB.query(new Query("DROP MEASUREMENT " + tablePrefix + "Meta"));
        influxDB.query(new Query("DROP MEASUREMENT " + tablePrefix + "Floor0"));
        influxDB.query(new Query("DROP MEASUREMENT " + tablePrefix + "Floor1"));
        influxDB.query(new Query("DROP MEASUREMENT " + tablePrefix + "Floor2"));
        influxDB.query(new Query("DROP MEASUREMENT " + tablePrefix + "Floor3"));
        influxDB.query(new Query("DROP MEASUREMENT " + tablePrefix + "Floor4"));
        influxDB.query(new Query("DROP MEASUREMENT " + tablePrefix + "Floor5"));
        influxDB.query(new Query("DROP MEASUREMENT " + tablePrefix + "Floor6"));
        influxDB.query(new Query("DROP MEASUREMENT " + tablePrefix + "Floor7"));
        influxDB.query(new Query("DROP MEASUREMENT " + tablePrefix + "Floor8"));
        influxDB.query(new Query("DROP MEASUREMENT " + tablePrefix + "Floor9"));
        influxDB.query(new Query("DROP MEASUREMENT " + tablePrefix + "Floor10"));
        influxDB.query(new Query("DROP MEASUREMENT " + tablePrefix + "Floor11"));
        influxDB.query(new Query("DROP MEASUREMENT " + tablePrefix + "Floor12"));
        influxDB.query(new Query("DROP MEASUREMENT " + tablePrefix + "Floor13"));
        influxDB.query(new Query("DROP MEASUREMENT " + tablePrefix + "Floor14"));
        influxDB.query(new Query("DROP MEASUREMENT " + tablePrefix + "Floor15"));
        return influxDB;
    }
}
