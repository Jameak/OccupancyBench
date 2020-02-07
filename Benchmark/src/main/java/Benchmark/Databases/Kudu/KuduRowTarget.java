package Benchmark.Databases.Kudu;

import Benchmark.Config.ConfigFile;
import Benchmark.Generator.GeneratedData.GeneratedRowEntry;
import Benchmark.Generator.GeneratedData.IGeneratedEntry;
import org.apache.kudu.client.*;

import java.io.IOException;

public class KuduRowTarget extends AbstractKuduTarget {
    private final KuduClient kuduClient;
    private final KuduTable kuduTable;
    private final KuduSession kuduSession;

    public KuduRowTarget(ConfigFile config, boolean recreate) throws KuduException {
        super(config);
        this.kuduClient = KuduHelper.openConnection(config);
        if(recreate){
            KuduHelper.deleteTable(kuduClient, config);
            KuduHelper.createTableWithRowSchema(kuduClient, config);
        }
        this.kuduTable = kuduClient.openTable(config.getKuduTable());
        this.kuduSession = kuduClient.newSession();
        // We could probably rely on one of the automatic modes, but the java examples talk about how the
        // time-based flushing isn't quite reliable so we'll go manual for now.
        kuduSession.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
        kuduSession.setMutationBufferSpace(config.getKuduMutationBufferSpace());
    }

    @Override
    public void add(IGeneratedEntry entry) throws IOException {
        assert entry instanceof GeneratedRowEntry : "Generated entry passed to row target must be a row-entry";
        GeneratedRowEntry rowEntry = (GeneratedRowEntry) entry;

        Insert insert = kuduTable.newInsert();
        PartialRow row = insert.getRow();
        assert insert.getTable().getSchema().getColumnId("time") == 0 : "Has the Kudu row-schema been changed?";
        row.addLong(0, padTime(rowEntry));
        assert insert.getTable().getSchema().getColumnId("AP") == 1 : "Has the Kudu row-schema been changed?";
        row.addString(1, rowEntry.getAP());
        assert insert.getTable().getSchema().getColumnId("clients") == 2 : "Has the Kudu row-schema been changed?";
        row.addInt(2, rowEntry.getNumClients());

        doInsert(kuduSession, insert);
    }

    @Override
    public void close() throws KuduException {
        kuduSession.close();
        kuduClient.close();
    }
}
