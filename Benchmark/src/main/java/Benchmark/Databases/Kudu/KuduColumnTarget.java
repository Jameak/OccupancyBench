package Benchmark.Databases.Kudu;

import Benchmark.Config.ConfigFile;
import Benchmark.Generator.GeneratedData.AccessPoint;
import Benchmark.Generator.GeneratedData.GeneratedColumnEntry;
import Benchmark.Generator.GeneratedData.IGeneratedEntry;
import org.apache.kudu.client.*;

import java.sql.Timestamp;

public class KuduColumnTarget extends AbstractKuduTarget {
    private final KuduClient kuduClient;
    private final AccessPoint[] allAPs;
    private final KuduTable kuduTable;
    private final KuduSession kuduSession;

    public KuduColumnTarget(ConfigFile config, boolean recreate, AccessPoint[] allAPs) throws KuduException {
        super(config);
        this.kuduClient = KuduHelper.openConnection(config);
        this.allAPs = allAPs;

        if(recreate){
            KuduHelper.deleteTable(kuduClient, config);
            KuduHelper.createTableWithColumnSchema(kuduClient, config, allAPs);
        }
        this.kuduTable = kuduClient.openTable(config.getKuduTable());
        this.kuduSession = kuduClient.newSession();
        // We could probably rely on one of the automatic modes, but the java examples talk about how the
        // time-based flushing isn't quite reliable so we'll go manual for now.
        kuduSession.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
        kuduSession.setMutationBufferSpace(config.getKuduMutationBufferSpace());
    }

    @Override
    public void add(IGeneratedEntry entry) throws KuduException {
        assert entry instanceof GeneratedColumnEntry : "Generated entry passed to column target must be a column-entry";
        GeneratedColumnEntry columnEntry = (GeneratedColumnEntry) entry;

        Insert insert = kuduTable.newInsert();
        PartialRow row = insert.getRow();
        assert insert.getTable().getSchema().getColumnId("time") == 0 : "Has the Kudu row-schema been changed?";
        row.addTimestamp(0, new Timestamp(padTime(columnEntry)));
        for(AccessPoint AP : allAPs){
            row.addInt(AP.getAPname(), columnEntry.getMapping().getOrDefault(AP.getAPname(), 0));
        }

        doInsert(kuduSession, insert);
    }

    @Override
    public void close() throws KuduException {
        kuduSession.close();
        kuduClient.close();
    }
}
