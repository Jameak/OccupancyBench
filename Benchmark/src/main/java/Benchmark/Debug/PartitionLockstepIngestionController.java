package Benchmark.Debug;

import Benchmark.Config.ConfigFile;
import Benchmark.Databases.DBTargets;
import Benchmark.Databases.Kudu.KuduPartitionInterval;
import Benchmark.Databases.Kudu.KuduPartitionType;
import Benchmark.Generator.GeneratedData.IGeneratedEntry;
import Benchmark.Generator.Targets.ITarget;
import Benchmark.PreciseTimer;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;

/**
 * Ingest-handler class for the partition lockstep proxy debug option that can be injected
 * into the normal ingestion-loop through composition.
 *
 * This class controls how many partitions to insert between running a specified
 * number of queries and writes the resulting data to a csv-file.
 */
public class PartitionLockstepIngestionController implements ITarget {
    private static final int PARTITIONS_BETWEEN_MEASUREMENTS = 10;
    public static final int QUERIES_PER_STOPPING_POINT = 5;
    //TODO: If we add support for changing the partitioning-interval, then this should reflect that instead of being hardcoded to the default interval.
    private static final int DAYS_PER_PARTITION = 7;
    private static final String OUT_FILENAME_PREFIX = "partition_lockstep_";
    private static final char SEPARATOR = '\t';
    private final String HEADER = "Time since start (sec)" + SEPARATOR + "Partitions since start" + SEPARATOR +
                                  "Query" + SEPARATOR + "Planning time (ms)" + SEPARATOR + "Execution time (ms)" + SEPARATOR +
                                  "Total time (ms)";

    private final PartitionLockstepChannel channel;
    private final PreciseTimer timer;
    private final PrintWriter fileOut;

    private LocalDateTime startDate;
    private LocalDateTime nextStopDate;
    private int partitionsSinceStart = 0;

    public PartitionLockstepIngestionController(ConfigFile config, PartitionLockstepChannel channel){
        if(config.getQueriesTarget() == DBTargets.KUDU){
            if(config.getKuduPartitionInterval() != KuduPartitionInterval.WEEKLY){
                throw new IllegalStateException("This debug option is hardcoded to expect a 7-day partition-interval since that's the default for the other databases.");
            }
            if(!(config.getKuduPartitionType() == KuduPartitionType.RANGE || config.getKuduPartitionType() == KuduPartitionType.HASH_AND_RANGE)){
                throw new IllegalStateException("Kudu partitioning is not setup to use range-partitioning, so partition lockstepping makes little sense");
            }
        }

        this.channel = channel;
        this.timer = new PreciseTimer();
        try {
            this.fileOut = new PrintWriter(OUT_FILENAME_PREFIX + config.getCsvFolderPrefix() + config.hashCode() + ".csv");
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void add(IGeneratedEntry entry) {
        if(startDate == null) {
            startDate = entry.getDateTime();
            timer.start();
            fileOut.println(HEADER);
            nextStopDate = startDate.plus(DAYS_PER_PARTITION * PARTITIONS_BETWEEN_MEASUREMENTS, ChronoUnit.DAYS);
        }

        if(!entry.getDateTime().isBefore(nextStopDate)){
            partitionsSinceStart += PARTITIONS_BETWEEN_MEASUREMENTS;

            channel.awaitInsert("INGEST");
            channel.awaitQueries("INGEST");

            System.out.println("INGEST: Entry date " + entry.getDateTime());
            write();

            nextStopDate = nextStopDate.plus(DAYS_PER_PARTITION * PARTITIONS_BETWEEN_MEASUREMENTS, ChronoUnit.DAYS);
        }
    }

    private void write(){
        ArrayList<PartitionLockstepChannel.QueryDuration> unreadDurations = channel.readAndReset();

        for(PartitionLockstepChannel.QueryDuration duration : unreadDurations){
            String line = String.format("%.3f", timer.elapsedSeconds()) +
                    SEPARATOR +
                    partitionsSinceStart +
                    SEPARATOR +
                    duration.queryName +
                    SEPARATOR +
                    (duration.detailed ? duration.planningTime : "") +
                    SEPARATOR +
                    (duration.detailed ? duration.executionTime : "") +
                    SEPARATOR +
                    duration.totalTime;

            fileOut.println(line);
            fileOut.flush();

            System.out.println(line);
        }
    }

    @Override
    public boolean shouldStopEarly() {
        return false;
    }

    @Override
    public void close() {
        write();
        fileOut.close();
    }
}
