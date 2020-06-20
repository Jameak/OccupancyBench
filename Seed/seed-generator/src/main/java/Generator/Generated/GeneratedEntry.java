package Generator.Generated;

import java.time.LocalDateTime;
import java.util.Map;

public class GeneratedEntry {
    public final LocalDateTime time;
    public final int totalClients;
    public final Map<String, Double> data;

    public GeneratedEntry(LocalDateTime time, int totalClients, Map<String, Double> data){
        this.time = time;
        this.totalClients = totalClients;
        this.data = data;
    }
}
