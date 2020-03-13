package Benchmark.Databases.Kudu;

import Benchmark.Config.ConfigFile;
import Benchmark.Config.Granularity;
import Benchmark.Generator.GeneratedData.IGeneratedEntry;
import Benchmark.Generator.Targets.ITarget;
import Benchmark.Logger;
import org.apache.kudu.client.Insert;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.OperationResponse;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.TimeUnit;

public abstract class AbstractKuduTarget implements ITarget {
    protected final Granularity granularity;
    protected final int batchSize;
    protected boolean errorOccured = false;
    private int inserts = 0;
    private final long lastDateWithPartition;
    private boolean hasWarnedAboutPartitionDate = false;

    public AbstractKuduTarget(ConfigFile config){
        // Kudu granularity is microseconds at best.
        this.granularity = config.getGeneratorGranularity() == Granularity.NANOSECOND
                ? Granularity.MICROSECOND : config.getGeneratorGranularity();

        this.batchSize = config.getKuduBatchSize();

        // We only pre-create a set number of range partitions, so warn when we exceed them.
        if(config.getKuduPartitionType() == KuduPartitionType.RANGE || config.getKuduPartitionType() == KuduPartitionType.HASH_AND_RANGE){
            LocalDate startDate = config.getGeneratorStartDate().isBefore(config.getIngestStartDate()) ? config.getGeneratorStartDate() : config.getIngestStartDate();
            LocalDateTime lastDate = startDate.atTime(0,0,0).plusYears(config.getKuduRangePrecreatedNumberOfYears());
            lastDateWithPartition = Granularity.MICROSECOND.getTime(lastDate);
        } else {
            lastDateWithPartition = Long.MAX_VALUE;
        }

    }

    protected void doInsert(KuduSession session, Insert insert) throws KuduException{
        try{
            OperationResponse response = session.apply(insert);
            inserts++;
            if(response != null && response.hasRowError()){
                errorOccured = true;
                Logger.LOG("KUDU: Error occurred during insertion apply. Error was: " + response.getRowError().toString());
            }
        } catch (KuduException e){
            Logger.LOG("KUDU: Exception raised while applying insert. Likely caused by the flush-buffer being full. Reduce batch size or increase mutation space.");
            throw e;
        }

        if(inserts == batchSize){
            inserts = 0;
            List<OperationResponse> responses = session.flush();
            for(OperationResponse response : responses){
                if(response.hasRowError()){
                    errorOccured = true;
                    Logger.LOG("KUDU: Error occurred during insertion flush. Error was: " + response.getRowError().toString());
                }
            }
        }
    }

    @Override
    public boolean shouldStopEarly() {
        return errorOccured;
    }

    protected long padTime(IGeneratedEntry entry){
        // We want a long of microseconds, so we need to pad to that precision regardless of
        //   the desired granularity, so first we truncate and then pad if needed.
        long granularTime = entry.getTime(granularity);
        long timestamp = TimeUnit.MICROSECONDS.convert(granularTime, granularity.toTimeUnit());
        if(timestamp > lastDateWithPartition && !hasWarnedAboutPartitionDate){
            hasWarnedAboutPartitionDate = true;
            Logger.LOG("Kudu target implementation has exceeded the pre-created range partitions. All further inserts will be put in the last, unbounded partition.");
        }
        return timestamp;
    }
}
