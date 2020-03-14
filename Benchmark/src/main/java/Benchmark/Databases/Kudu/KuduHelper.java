package Benchmark.Databases.Kudu;

import Benchmark.Config.ConfigFile;
import Benchmark.Config.Granularity;
import Benchmark.Generator.GeneratedData.AccessPoint;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;

import java.time.LocalDate;
import java.time.LocalDateTime;
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
        return new KuduClient.KuduClientBuilder(kuduMasters).defaultAdminOperationTimeoutMs(120000).build();
    }

    public static void createTableWithRowSchema(KuduClient client, ConfigFile config) throws KuduException {
        String table = config.getKuduTable();
        List<ColumnSchema> columns = new ArrayList<>();
        columns.add(
                new ColumnSchema.ColumnSchemaBuilder("time", Type.UNIXTIME_MICROS)
                        .key(true)
                        .encoding(ColumnSchema.Encoding.BIT_SHUFFLE)
                        .nullable(false)
                        .build()
        );
        columns.add(
                new ColumnSchema.ColumnSchemaBuilder("AP", Type.STRING)
                        .key(true)
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

        Schema schema = new Schema(columns);
        CreateTableOptions options = new CreateTableOptions();
        options.setNumReplicas(1);
        List<String> hashKeys = new ArrayList<>();
        hashKeys.add("AP");
        List<String> rangeKeys = new ArrayList<>();
        rangeKeys.add("time");

        switch (config.getKuduPartitionType()){
            case NONE:
                // The table partition column must be specified even if we dont use it (in which case Kudu creates
                // 1 unbounded partition that everything ends up in)
                options.setRangePartitionColumns(rangeKeys);
                client.createTable(table, schema, options);
                break;
            case HASH:
                options.addHashPartitions(hashKeys, config.getKuduHashBuckets());
                client.createTable(table, schema, options);
                break;
            case RANGE:
            {
                // Kudu limits the number of tablets (including replicas?) created by a createTable call to 300.
                // Depending on the configured tablet-replication number (which isn't controlled by this benchmark) and
                // depending on how many years of partitions we pre-create, we can very easily exceed that 300 tablet limit.
                // So we create the table with the default, unbounded, range-partition and then drop it immediately after.

                options.setRangePartitionColumns(rangeKeys);
                client.createTable(table, schema, options);
                AlterTableOptions alterTableOptions = new AlterTableOptions();
                alterTableOptions.dropRangePartition(schema.newPartialRow(), schema.newPartialRow());
                createRangePartitions(config, schema, alterTableOptions);
                client.alterTable(table, alterTableOptions);
                break;
            }
            case HASH_AND_RANGE:
            {
                // See RANGE comment above.
                // Additionally, the behavior for hash + range partitions means that we exceed that 300 tablet max much faster
                // since the number of tablets will be "hash buckets * range partitions" (multiplied with tablet replica amount?)
                options.setRangePartitionColumns(rangeKeys);
                options.addHashPartitions(hashKeys, config.getKuduHashBuckets());
                client.createTable(table, schema, options);

                AlterTableOptions alterTableOptions = new AlterTableOptions();
                alterTableOptions.dropRangePartition(schema.newPartialRow(), schema.newPartialRow());
                createRangePartitions(config, schema, alterTableOptions);
                client.alterTable(table, alterTableOptions);
            }
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + config.getKuduPartitionType());
        }
    }

    public static void createTableWithColumnSchema(KuduClient client, ConfigFile config, AccessPoint[] allAPs) throws KuduException {
        String table = config.getKuduTable();

        List<ColumnSchema> columns = new ArrayList<>();
        columns.add(
                new ColumnSchema.ColumnSchemaBuilder("time", Type.UNIXTIME_MICROS)
                        .key(true)
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

        Schema schema = new Schema(columns);
        CreateTableOptions options = new CreateTableOptions();
        options.setNumReplicas(1);
        List<String> rangeKeys = new ArrayList<>();
        rangeKeys.add("time");

        switch (config.getKuduPartitionType()){
            case NONE:
                // The table partition column must be specified even if we dont use it (in which case Kudu creates
                // 1 unbounded partition that everything ends up in)
                options.setRangePartitionColumns(rangeKeys);
                client.createTable(table, schema, options);
                break;
            case RANGE:
                // Kudu limits the number of tablets (including replicas?) created by a createTable call to 300.
                // Depending on the configured tablet-replication number (which isn't controlled by this benchmark) and
                // depending on how many years of partitions we pre-create, we can very easily exceed that 300 tablet limit.
                // So we create the table with the default, unbounded, range-partition and then drop it immediately after.
                options.setRangePartitionColumns(rangeKeys);
                client.createTable(table, schema, options);

                AlterTableOptions alterTableOptions = new AlterTableOptions();
                alterTableOptions.dropRangePartition(schema.newPartialRow(), schema.newPartialRow());
                createRangePartitions(config, schema, alterTableOptions);
                client.alterTable(table, alterTableOptions);
                break;
            case HASH_AND_RANGE:
            case HASH:
                throw new UnsupportedOperationException("Kudu column-schema doesn't have a row that eligible for hash-partitioning. Use range-partitioning or no partition.");
            default:
                throw new IllegalStateException("Unexpected value: " + config.getKuduPartitionType());
        }
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

    private static void createRangePartitions(ConfigFile config, Schema schema, AlterTableOptions options){
        LocalDate startDate = config.getGeneratorStartDate().isBefore(config.getIngestStartDate()) ? config.getGeneratorStartDate() : config.getIngestStartDate();
        LocalDateTime startDateWithTime = startDate.atTime(0,0,0);

        // Create an unbounded range-partition from [-INFINITY, startDate).
        // Shouldn't strictly be needed since nothing should ever insert into it or query it, but just in case:
        PartialRow unboundedStart = schema.newPartialRow();
        unboundedStart.addLong("time", Granularity.MICROSECOND.getTime(startDateWithTime));
        options.addRangePartition(schema.newPartialRow(),unboundedStart);

        LocalDateTime date = startDateWithTime;
        while(date.isBefore(startDateWithTime.plusYears(config.getKuduRangePrecreatedNumberOfYears()))){
            LocalDateTime next = date;
            switch (config.getKuduPartitionInterval()){
                case WEEKLY:
                    next = next.plusDays(7);
                    break;
                case MONTHLY:
                    next = next.plusMonths(1);
                    break;
                default:
                    throw new IllegalStateException("Unexpected value: " + config.getKuduPartitionInterval());
            }

            PartialRow startRow = schema.newPartialRow();
            PartialRow endRow = schema.newPartialRow();
            startRow.addLong("time", Granularity.MICROSECOND.getTime(date));
            endRow.addLong("time", Granularity.MICROSECOND.getTime(next));
            options.addRangePartition(startRow, endRow);

            date = next;
        }

        // Create an unbounded range-partition from [startDate + x years, +INFINITY).
        // Might be needed if ingestion/generation runs for long enough that the defined partitions are exceeded.
        // Without this, we would crash when we reach this point. The Kudu ITarget-implementations contain code that'll
        // warn about hitting this last partition, giving the user more options than simply straight crashing.
        PartialRow unboundedEnd = schema.newPartialRow();
        unboundedEnd.addLong("time", Granularity.MICROSECOND.getTime(date));
        options.addRangePartition(unboundedEnd, schema.newPartialRow());
    }
}
