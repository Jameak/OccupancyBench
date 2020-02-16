package Benchmark.Queries.Results;

import java.util.Arrays;
import java.util.Set;

public class KMeans extends AbstractResult {
    private final Set<String> assignedAPs;

    public KMeans(Set<String> assignedAPs){
        this.assignedAPs = assignedAPs;
    }

    @Override
    public String print() {
        if(assignedAPs.isEmpty()) return "EMPTY";

        String[] APs = assignedAPs.toArray(new String[0]);
        Arrays.sort(APs);

        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for(String AP : APs){
            if(first) {
                first = false;
            } else {
                sb.append(";");
            }
            sb.append(AP);
        }
        return sb.toString();
    }
}
