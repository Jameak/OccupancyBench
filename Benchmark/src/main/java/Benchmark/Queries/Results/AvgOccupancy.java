package Benchmark.Queries.Results;

import java.util.Locale;

/**
 * Stores the results of an Avg Occupancy query.
 */
public class AvgOccupancy extends AbstractResult{
    private final String AP;
    private final int currentClients;
    private final double historical_clients_now;
    private final double historical_clients_soon;

    public AvgOccupancy(String AP, int currentClients, double historical_clients_now, double historical_clients_soon){
        this.AP = AP;
        this.currentClients = currentClients;
        this.historical_clients_now = historical_clients_now;
        this.historical_clients_soon = historical_clients_soon;
    }

    @Override
    public String print() {
        return String.format(Locale.US, "%s;%d;%.2f;%.2f", AP, currentClients, historical_clients_now, historical_clients_soon);
    }
}
