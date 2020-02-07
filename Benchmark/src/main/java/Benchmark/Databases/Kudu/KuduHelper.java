package Benchmark.Databases.Kudu;

import Benchmark.Config.ConfigFile;
import Benchmark.Generator.GeneratedData.AccessPoint;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;

import java.util.ArrayList;
import java.util.List;

/**
 * Contains static helper functions used by the Kudu-database implementations.
 *
 * Documentation for column encodings: https://kudu.apache.org/releases/0.6.0/docs/schema_design.html
 */
public class KuduHelper {
    public static KuduClient openConnection(ConfigFile config){
        String kuduMasters = config.getKuduMasters();
        return new KuduClient.KuduClientBuilder(kuduMasters).build();
    }

    public static void createTableWithRowSchema(KuduClient client, ConfigFile config) throws KuduException {
        String table = config.getKuduTable();
        List<ColumnSchema> columns = new ArrayList<>();
        columns.add(
                new ColumnSchema.ColumnSchemaBuilder("time", Type.UNIXTIME_MICROS)
                        .key(true)
                        //TODO: Is this the best encoding for this? This is the encoding used for the timestamp
                        //      in the 'collectl' Kudu example code.
                        .encoding(ColumnSchema.Encoding.BIT_SHUFFLE)
                        .nullable(false)
                        .build()
        );
        columns.add(
                new ColumnSchema.ColumnSchemaBuilder("AP", Type.STRING)
                        .key(false)
                        .encoding(ColumnSchema.Encoding.DICT_ENCODING)
                        .nullable(false)
                        .build()
        );
        columns.add(
                new ColumnSchema.ColumnSchemaBuilder("clients", Type.INT32)
                        .key(false)
                        .encoding(ColumnSchema.Encoding.PLAIN_ENCODING)
                        .nullable(false)
                        .build()
        );

        List<String> rangeKeys = new ArrayList<>();
        rangeKeys.add("time");

        //TODO: Kudu doesn't do range-partitioning by default, we just tell it what column to partition on here.
        //      We need to choose some default interval that varies based on amounts of data (e.g. a split per week, per month, or per year)
        //      or whether all those ranges will need to be setup here at table-creation, and/or during insertion as
        //      inserted timestamps expand beyond the initially defined partitions.
        //TODO: If we need to handle partitioning ourselves, then they can be added/dropped after creation: https://kudu.apache.org/docs/schema_design.html#alter-schema
        client.createTable(table, new Schema(columns), new CreateTableOptions().setRangePartitionColumns(rangeKeys));
    }

    public static void createTableWithColumnSchema(KuduClient client, ConfigFile config, AccessPoint[] allAPs) throws KuduException {
        String table = config.getKuduTable();

        List<ColumnSchema> columns = new ArrayList<>();
        columns.add(
                new ColumnSchema.ColumnSchemaBuilder("time", Type.UNIXTIME_MICROS)
                        .key(true)
                        //TODO: Is this the best encoding for this? This is the encoding used for the timestamp
                        //      in the 'collectl' Kudu example code.
                        .encoding(ColumnSchema.Encoding.BIT_SHUFFLE)
                        .nullable(false)
                        .build()
        );
        for(AccessPoint AP : allAPs){
            columns.add(
                    new ColumnSchema.ColumnSchemaBuilder(AP.getAPname(), Type.INT32)
                            .key(false)
                            .encoding(ColumnSchema.Encoding.PLAIN_ENCODING)
                            // The other column-schema implementations dont let these be nullable.
                            // We could easily support it, but then databases wouldn't be as comparable
                            // as having identical nullable behavior.
                            .nullable(false)
                            .build()
            );
        }

        if(columns.size() > config.getKuduMaxColumns()){
            throw new IllegalStateException("Too many columns are needed to store the chosen config in a column-format. Reduce the scaling or switch to a row-format.");
        }

        List<String> rangeKeys = new ArrayList<>();
        rangeKeys.add("time");

        //TODO: Kudu doesn't do range-partitioning by default, we just tell it what column to partition on here.
        //      We need to choose some default interval that varies based on amounts of data (e.g. a split per week, per month, or per year)
        //      or whether all those ranges will need to be setup here at table-creation, and/or during insertion as
        //      inserted timestamps expand beyond the initially defined partitions.
        client.createTable(table, new Schema(columns), new CreateTableOptions().setRangePartitionColumns(rangeKeys));
    }

    public static void deleteTable(KuduClient client, ConfigFile config) {
        try {
            client.deleteTable(config.getKuduTable());
        } catch (KuduException e) {
            // I assume that this exception is thrown if the table doesn't exist.
            // If this error was caused by something else, then that same exception should be thrown when
            // we try to create the table again, and that one should get floated so this should be fine.
        }
    }
}
