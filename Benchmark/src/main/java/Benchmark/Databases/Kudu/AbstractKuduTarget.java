package Benchmark.Databases.Kudu;

import Benchmark.Config.ConfigFile;
import Benchmark.Generator.GeneratedData.IGeneratedEntry;
import Benchmark.Generator.Targets.ITarget;
import Benchmark.Logger;
import org.apache.kudu.client.Insert;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.OperationResponse;

import java.util.List;
import java.util.concurrent.TimeUnit;

public abstract class AbstractKuduTarget implements ITarget {
    protected final ConfigFile.Granularity granularity;
    protected final int batchSize;
    protected boolean errorOccured = false;
    private int inserts = 0;

    public AbstractKuduTarget(ConfigFile config){
        // Note: Kudu column setup is specified as UNIXTIME_MICROS so granularity is microseconds at best.
        //       However the time-stamp constructor expects milliseconds, so that's our best granularity-option.
        //       TODO: Mention this in the config documentation for granularity as well.
        this.granularity = config.getGeneratorGranularity() == ConfigFile.Granularity.NANOSECOND || config.getGeneratorGranularity() == ConfigFile.Granularity.MILLISECOND
                ? ConfigFile.Granularity.MILLISECOND : config.getGeneratorGranularity();

        this.batchSize = config.getKuduBatchSize();
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
        // We want a long of milliseconds, so we need to pad to that precision regardless of
        //   the desired granularity, so first we truncate and then pad if needed.
        long granularTime = entry.getTime(granularity);
        return granularity.toTimeUnit().convert(granularTime, TimeUnit.MILLISECONDS);
    }
}
