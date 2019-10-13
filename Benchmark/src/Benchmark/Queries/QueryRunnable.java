package Benchmark.Queries;

import Benchmark.CoarseTimer;
import Benchmark.Config.ConfigFile;
import Benchmark.Generator.GeneratedData.Floor;
import Benchmark.Logger;
import Benchmark.PreciseTimer;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.Random;

/**
 * The top-level class for benchmark queries. Monitors query-performance, decides which queries to run
 * and generates their arguments.
 */
public class QueryRunnable implements Runnable {
    private final ConfigFile config;
    private final Random rng;
    private final Logger logger;
    private final Floor[] generatedFloors;
    private final String threadName;
    private final int minTimeInterval;
    private final int maxTimeInterval;

    private final PreciseTimer timerQuery_TotalClients;
    private final PreciseTimer timerQuery_FloorTotal;

    private final int totalThreshold;
    private final int thresholdQuery_TotalClients;
    private final int thresholdQuery_FloorTotal;

    private int countFull;
    private int countQuery_TotalClients;
    private int countQuery_FloorTotal;

    private long timeSpentQuery_TotalClients;
    private long timeSpentQuery_FloorTotal;

    public QueryRunnable(ConfigFile config, Random rng, Logger logger, Floor[] generatedFloors, String threadName){
        this.config = config;
        this.rng = rng;
        this.logger = logger;
        this.generatedFloors = generatedFloors;
        this.threadName = threadName;
        this.timerQuery_TotalClients = new PreciseTimer();
        this.timerQuery_FloorTotal = new PreciseTimer();

        assert this.config.queriesPropTotalClients() >= 0;
        this.thresholdQuery_TotalClients = config.queriesPropTotalClients();
        assert this.config.queriesPropFloorTotals() >= 0;
        this.thresholdQuery_FloorTotal = config.queriesPropTotalClients() + config.queriesPropFloorTotals();
        this.totalThreshold = config.queriesPropTotalClients() + config.queriesPropFloorTotals();

        this.minTimeInterval = config.queriesIntervalMin();
        this.maxTimeInterval = config.queriesIntervalMax();
    }

    private QueryType selectQuery(){
        int val = rng.nextInt(totalThreshold);
        if(val < thresholdQuery_TotalClients) return QueryType.TotalClients;
        else if (val < thresholdQuery_FloorTotal) return QueryType.FloorTotals;
        return QueryType.UNKNOWN;
    }

    private void runQueries(Queries queries, boolean warmUp){
        switch (selectQuery()){
            case TotalClients:
            {
                LocalDateTime[] time = generateTime();
                if(!warmUp) timerQuery_TotalClients.start();
                queries.computeTotalClients(time[0], time[1]);
                if(!warmUp) {
                    timeSpentQuery_TotalClients += timerQuery_TotalClients.elapsedNanoseconds();
                    countQuery_TotalClients++;
                }
            }
                break;
            case FloorTotals:
            {
                LocalDateTime[] time = generateTime();
                if(!warmUp) timerQuery_FloorTotal.start();
                queries.computeFloorTotal(time[0], time[1], generatedFloors);
                if(!warmUp){
                    timeSpentQuery_FloorTotal += timerQuery_FloorTotal.elapsedNanoseconds();
                    countQuery_FloorTotal++;
                }
            }
                break;
            case UNKNOWN:
                assert false;
                logger.log(threadName + ": WTF. Invalid query probabilities");
                throw new RuntimeException();
        }
        if(!warmUp) countFull++;
    }

    private LocalDateTime[] generateTime(){
        LocalDate earliestValidDate = config.queriesEarliestValidDate();
        LocalDateTime startClamp = LocalDateTime.of(earliestValidDate, LocalTime.of(0,0,0));
        LocalDateTime[] time = new LocalDateTime[2];
        LocalDateTime newestValidDate = logger.getNewestTime();

        LocalDateTime time1 = generateRandomTime(newestValidDate, startClamp);
        LocalDateTime time2 = generateRandomTime(newestValidDate, startClamp);
        if(time1.isBefore(time2)){
            time[0] = time1;
            time[1] = time2;
        } else {
            time[1] = time1;
            time[0] = time2;
        }

        if(time[0].isBefore(startClamp))     time[0] = startClamp;
        if(time[1].isAfter(newestValidDate)) time[1] = newestValidDate;

        // Make sure that the interval obeys the minimum size. If the interval is smaller than minimum size, then we bias towards the newest values.
        if(time[0].plusSeconds(minTimeInterval).isAfter(time[1])){
            time[1] = time[0].plusSeconds(minTimeInterval);
            if(time[1].isAfter(newestValidDate)) {
                time[1] = newestValidDate;
                time[0] = newestValidDate.minusSeconds(minTimeInterval);
                if(time[0].isBefore(startClamp)) time[0] = startClamp;
            }
        }

        // Make sure that the interval obeys the maximum size. If the interval is larger than the maximum size, then we bias towards the oldest values.
        if(time[0].plusSeconds(maxTimeInterval).isBefore(time[1])){
            time[1] = time[0].plusSeconds(maxTimeInterval);
        }

        assert !time[0].isBefore(startClamp);
        assert !time[1].isAfter(newestValidDate);
        return time;
    }

    private LocalDateTime generateRandomTime(LocalDateTime newestValue, LocalDateTime earliestValue){
        double choice = randomValue();
        if(choice < config.queriesRngRangeDay()){
            // Query for some time on the newest date.
            return randomTimeBetween(newestValue.minusDays(1), newestValue, earliestValue);
        } else if(choice < config.queriesRngRangeWeek()){
            // Query for some time within the last 7 days.
            return randomTimeBetween(newestValue.minusDays(6), newestValue, earliestValue);
        } else if(choice < config.queriesRngRangeMonth()){
            // Query: within the last 30 days
            return randomTimeBetween(newestValue.minusDays(29), newestValue, earliestValue);
        } else if(choice < config.queriesRngRangeYear()){
            // Query: within the last 365 days
            return randomTimeBetween(newestValue.minusDays(364), newestValue, earliestValue);
        } else {
            // Query: any value within the valid interval
            return randomTimeBetween(earliestValue, newestValue, earliestValue);
        }
    }

    private LocalDateTime randomTimeBetween(LocalDateTime start, LocalDateTime end, LocalDateTime clamp){
        if(start.isBefore(clamp)) start = clamp;
        assert start.isBefore(end);
        assert clamp.isBefore(start) || clamp.isEqual(start);
        long startSeconds = start.toEpochSecond(ZoneOffset.ofHours(0));
        int diffSeconds = Math.toIntExact(end.toEpochSecond(ZoneOffset.ofHours(0)) - startSeconds);
        assert diffSeconds > 0;
        long randBetween = startSeconds + rng.nextInt(diffSeconds);
        return LocalDateTime.ofEpochSecond(randBetween, 0, ZoneOffset.ofHours(0));
    }

    private double randomValue(){
        // Generate a random value that follows an exponential distribution using the inversion method.
        int lambda = config.queriesRngLambda();
        return Math.log(1 - rng.nextDouble()) / -lambda;
    }

    private void reportStats(){
        double totalTime = (timeSpentQuery_TotalClients + timeSpentQuery_FloorTotal) / 1e9;
        logger.log(String.format("%s: Query name      |    Count |   Total time |  Queries / sec", threadName));
        logger.log(String.format("%s: 'Total Clients' | %8d | %8.1f sec | %8.1f / sec", threadName, countQuery_TotalClients, timeSpentQuery_TotalClients / 1e9, countQuery_TotalClients / (timeSpentQuery_TotalClients / 1e9) ));
        logger.log(String.format("%s: 'Floor Totals'  | %8d | %8.1f sec | %8.1f / sec", threadName, countQuery_FloorTotal, timeSpentQuery_FloorTotal / 1e9, countQuery_FloorTotal  / (timeSpentQuery_FloorTotal  / 1e9) ));
        logger.log(String.format("%s: ----------------|----------|--------------|---------------", threadName));
        logger.log(String.format("%s: TOTAL           | %8d | %8.1f sec | %8.1f / sec", threadName, countFull, totalTime, countFull / totalTime ));
    }

    @Override
    public void run() {
        int duration = config.queriesDuration();
        int warmUpTime = config.queriesWarmup();
        int targetCount = config.queriesMaxCount();
        CoarseTimer timer = new CoarseTimer();
        boolean warmUp = warmUpTime > 0;

        Queries queries = new InfluxQueries();
        try {
            queries.prepare(config);
        } catch (Exception e) {
            e.printStackTrace();
        }

        if (warmUp) {
            timer.start();
            while (timer.elapsedSeconds() < warmUpTime) {
                runQueries(queries, true);
            }
        }

        if(duration > 0){
            timer.start();
            while (timer.elapsedSeconds() < duration) {
                runQueries(queries, false);
            }
        } else {
            assert targetCount > 0;
            while (countFull < targetCount) {
                runQueries(queries, false);
            }
        }
        
        queries.done();
        logger.log(this::reportStats);
    }

    private enum QueryType{
        TotalClients, FloorTotals, UNKNOWN
    }
}
